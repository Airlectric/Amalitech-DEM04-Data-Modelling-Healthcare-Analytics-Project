-- ==================================================
-- ETL SCRIPT: Incremental Daily Load with Late-Arriving Facts
-- Source DB: hospital_db
-- Target DB: hospital_star_db
-- Run daily; handles updates and late facts via lookback
-- Assumes: last_updated columns exist in OLTP tables
-- Assumes: etl_control table exists for watermark and auditing
-- Assumes: dim_date is pre-loaded (static)
-- ==================================================

USE hospital_star_db;

-- Step 1: Get last watermark and set variables
SET @last_watermark = (SELECT MAX(last_watermark) FROM etl_control WHERE status = 'SUCCESS');
SET @last_watermark = IFNULL(@last_watermark, '2000-01-01');  -- Default for first run
SET @lookback_days = 7;  -- Adjust for your data latency
SET @incremental_cutoff = @last_watermark;
SET @lookback_cutoff = DATE_SUB(CURDATE(), INTERVAL @lookback_days DAY);
SET @new_watermark = NOW();  -- Will update at end
SET @load_start_time = NOW();
SET @records_processed = 0;

-- Step 2: Incremental for dimensions (Type 1 upsert for most, Type 2 for patient)

-- dim_patient (SCD Type 2: Insert new/current versions only when needed)
INSERT INTO dim_patient (
    patient_id, first_name, last_name, date_of_birth, gender, mrn, age_group,
    start_date, end_date, is_current
)
SELECT 
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.mrn,
    CASE
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 18 THEN '0-18'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, CURDATE()) < 65 THEN '19-65'
        ELSE '65+'
    END AS age_group,
    CURDATE()          AS start_date,
    '9999-12-31'       AS end_date,
    TRUE               AS is_current
FROM hospital_db.patients p
LEFT JOIN dim_patient curr 
    ON curr.patient_id = p.patient_id 
   AND curr.is_current = TRUE
WHERE p.last_updated > @incremental_cutoff
  AND (
      -- New patient
      curr.patient_key IS NULL
      -- OR existing patient but attributes changed
      OR curr.first_name      != p.first_name
      OR curr.last_name       != p.last_name
      OR curr.date_of_birth   != p.date_of_birth
      OR curr.gender          != p.gender
      OR curr.mrn             != p.mrn
  );

-- Expire old version(s) when change detected
UPDATE dim_patient dp
INNER JOIN hospital_db.patients p 
    ON dp.patient_id = p.patient_id 
   AND dp.is_current = TRUE
SET 
    dp.end_date   = CURDATE() - INTERVAL 1 DAY,
    dp.is_current = FALSE
WHERE p.last_updated > @incremental_cutoff
  AND (
      dp.first_name      != p.first_name
      OR dp.last_name     != p.last_name
      OR dp.date_of_birth != p.date_of_birth
      OR dp.gender        != p.gender
      OR dp.mrn           != p.mrn
  );

-- dim_provider (Type 1 upsert, include specialty_key and department_key)
INSERT INTO dim_provider (provider_id, first_name, last_name, credential, specialty_key, department_key)
SELECT
    p.provider_id, p.first_name, p.last_name, p.credential,
    ds.specialty_key, dd.department_key
FROM hospital_db.providers p
JOIN dim_specialty ds ON ds.specialty_id = p.specialty_id
JOIN dim_department dd ON dd.department_id = p.department_id
WHERE p.last_updated > @incremental_cutoff
ON DUPLICATE KEY UPDATE
    first_name = VALUES(first_name), last_name = VALUES(last_name), credential = VALUES(credential),
    specialty_key = VALUES(specialty_key), department_key = VALUES(department_key);

-- dim_specialty (Type 1)
INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT s.specialty_id, s.specialty_name, s.specialty_code
FROM hospital_db.specialties s
WHERE s.last_updated > @incremental_cutoff
ON DUPLICATE KEY UPDATE
    specialty_name = VALUES(specialty_name), specialty_code = VALUES(specialty_code);

-- dim_department (Type 1)
INSERT INTO dim_department (department_id, department_name, floor, capacity)
SELECT d.department_id, d.department_name, d.floor, d.capacity
FROM hospital_db.departments d
WHERE d.last_updated > @incremental_cutoff
ON DUPLICATE KEY UPDATE
    department_name = VALUES(department_name), floor = VALUES(floor), capacity = VALUES(capacity);

-- dim_encounter_type (Type 1, from distinct)
INSERT INTO dim_encounter_type (type_name)
SELECT DISTINCT e.encounter_type
FROM hospital_db.encounters e
WHERE e.last_updated > @incremental_cutoff AND e.encounter_type IS NOT NULL
ON DUPLICATE KEY UPDATE
    type_name = VALUES(type_name);

-- dim_diagnosis (Type 1)
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT d.diagnosis_id, d.icd10_code, d.icd10_description
FROM hospital_db.diagnoses d
WHERE d.last_updated > @incremental_cutoff
ON DUPLICATE KEY UPDATE
    icd10_code = VALUES(icd10_code), icd10_description = VALUES(icd10_description);

-- dim_procedure (Type 1)
INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT pr.procedure_id, pr.cpt_code, pr.cpt_description
FROM hospital_db.procedures pr
WHERE pr.last_updated > @incremental_cutoff
ON DUPLICATE KEY UPDATE
    cpt_code = VALUES(cpt_code), cpt_description = VALUES(cpt_description);

-- Step 3: Handle fact_encounters incrementally
-- Delete changed/late records (based on encounter or linked updates)
DELETE fe FROM fact_encounters fe
WHERE fe.encounter_id IN (
    SELECT e.encounter_id 
      FROM hospital_db.encounters e 
     WHERE e.last_updated > @lookback_cutoff 
        OR e.encounter_date > @lookback_cutoff
    UNION
    SELECT b.encounter_id 
      FROM hospital_db.billing b 
     WHERE b.last_updated > @lookback_cutoff
    UNION
    SELECT ed.encounter_id 
      FROM hospital_db.encounter_diagnoses ed 
     WHERE ed.last_updated > @lookback_cutoff
    UNION
    SELECT ep.encounter_id 
      FROM hospital_db.encounter_procedures ep 
     WHERE ep.last_updated > @lookback_cutoff
);

-- Insert new/updated facts
INSERT INTO fact_encounters (
    encounter_id, patient_key, provider_key, specialty_key, department_key,
    encounter_type_key, encounter_date_key, discharge_date_key, claim_date_key,
    diagnosis_count, procedure_count, total_allowed, claim_amount,
    encounter_duration_hours, is_readmission, load_date
)
SELECT
    e.encounter_id, dp.patient_key, dpr.provider_key, ds.specialty_key, dd.department_key,
    det.encounter_type_key, ddate.date_key, ddischarge.date_key, dclaim.date_key,
    COALESCE(diag_cnt.cnt, 0), COALESCE(proc_cnt.cnt, 0),
    COALESCE(b.allowed_amount, 0), COALESCE(b.claim_amount, 0),
    TIMESTAMPDIFF(HOUR, e.encounter_date, e.discharge_date),
    IF(EXISTS(SELECT 1 FROM hospital_db.encounters e2 WHERE e2.patient_id = e.patient_id
              AND e2.encounter_type = 'Inpatient' AND e2.encounter_date BETWEEN e.discharge_date AND DATE_ADD(e.discharge_date, INTERVAL 30 DAY)
              AND e2.encounter_id != e.encounter_id), TRUE, FALSE),
    CURDATE()
FROM hospital_db.encounters e
JOIN dim_patient dp ON dp.patient_id = e.patient_id AND dp.is_current = TRUE
JOIN dim_provider dpr ON dpr.provider_id = e.provider_id
JOIN hospital_db.providers p ON p.provider_id = e.provider_id  -- For specialty link
JOIN dim_specialty ds ON ds.specialty_id = p.specialty_id
JOIN dim_department dd ON dd.department_id = e.department_id
JOIN dim_encounter_type det ON det.type_name = e.encounter_type
JOIN dim_date ddate ON ddate.calendar_date = DATE(e.encounter_date)
JOIN dim_date ddischarge ON ddischarge.calendar_date = DATE(e.discharge_date)
LEFT JOIN hospital_db.billing b ON b.encounter_id = e.encounter_id
LEFT JOIN dim_date dclaim ON dclaim.calendar_date = b.claim_date
LEFT JOIN (SELECT encounter_id, COUNT(*) cnt FROM hospital_db.encounter_diagnoses GROUP BY encounter_id) diag_cnt ON diag_cnt.encounter_id = e.encounter_id
LEFT JOIN (SELECT encounter_id, COUNT(*) cnt FROM hospital_db.encounter_procedures GROUP BY encounter_id) proc_cnt ON proc_cnt.encounter_id = e.encounter_id
WHERE (e.last_updated > @incremental_cutoff OR e.encounter_date > @lookback_cutoff)
   OR (b.last_updated > @incremental_cutoff);  -- Include billing updates

SET @records_processed = @records_processed + ROW_COUNT();

-- Step 4: Handle bridge tables (delete and reinsert for changed encounters)
-- bridge_encounter_diagnoses
DELETE bed FROM bridge_encounter_diagnoses bed
JOIN fact_encounters fe ON bed.encounter_key = fe.encounter_key
WHERE fe.encounter_id IN (
    SELECT ed.encounter_id FROM hospital_db.encounter_diagnoses ed WHERE ed.last_updated > @lookback_cutoff
);

INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
SELECT fe.encounter_key, dd.diagnosis_key, MIN(ed.diagnosis_sequence)
FROM hospital_db.encounter_diagnoses ed
JOIN fact_encounters fe ON fe.encounter_id = ed.encounter_id
JOIN dim_diagnosis dd ON dd.diagnosis_id = ed.diagnosis_id
WHERE ed.last_updated > @incremental_cutoff OR fe.load_date = CURDATE()  -- Link to new facts
GROUP BY fe.encounter_key, dd.diagnosis_key
ON DUPLICATE KEY UPDATE diagnosis_sequence = VALUES(diagnosis_sequence);

-- bridge_encounter_procedures
DELETE bep FROM bridge_encounter_procedures bep
JOIN fact_encounters fe ON bep.encounter_key = fe.encounter_key
WHERE fe.encounter_id IN (
    SELECT ep.encounter_id FROM hospital_db.encounter_procedures ep WHERE ep.last_updated > @lookback_cutoff
);

INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key)
SELECT fe.encounter_key, dp.procedure_key, MIN(ddate.date_key)
FROM hospital_db.encounter_procedures ep
JOIN fact_encounters fe ON fe.encounter_id = ep.encounter_id
JOIN dim_procedure dp ON dp.procedure_id = ep.procedure_id
JOIN dim_date ddate ON ddate.calendar_date = ep.procedure_date
WHERE ep.last_updated > @incremental_cutoff OR fe.load_date = CURDATE()
GROUP BY fe.encounter_key, dp.procedure_key
ON DUPLICATE KEY UPDATE procedure_date_key = VALUES(procedure_date_key);

-- Step 5: Update control table (audit + watermark)
INSERT INTO etl_control (load_type, last_watermark, load_date, records_processed, status)
VALUES ('INCREMENTAL', @new_watermark, @load_start_time, @records_processed, 'SUCCESS');
