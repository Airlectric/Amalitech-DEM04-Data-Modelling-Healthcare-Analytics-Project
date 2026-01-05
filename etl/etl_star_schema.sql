-- ==================================================
-- ETL SCRIPT: Load Star Schema from OLTP Tables
-- This script performs a FULL REFRESH of the data warehouse
-- ==================================================

-- Step 1: Truncate all star schema tables (for full refresh)
TRUNCATE TABLE bridge_encounter_procedures;
TRUNCATE TABLE bridge_encounter_diagnoses;
TRUNCATE TABLE fact_encounters;
TRUNCATE TABLE dim_date;
TRUNCATE TABLE dim_patient;
TRUNCATE TABLE dim_provider;
TRUNCATE TABLE dim_specialty;
TRUNCATE TABLE dim_department;
TRUNCATE TABLE dim_encounter_type;
TRUNCATE TABLE dim_diagnosis;
TRUNCATE TABLE dim_procedure;

-- Step 2: Load dim_date (1900 – 2100)
INSERT INTO dim_date (calendar_date, year, month, quarter, day_of_week, is_holiday)
SELECT 
    d.date_val AS calendar_date,
    YEAR(d.date_val) AS year,
    MONTH(d.date_val) AS month,
    QUARTER(d.date_val) AS quarter,
    WEEKDAY(d.date_val) + 1 AS day_of_week,  -- 1=Monday ... 7=Sunday
    FALSE AS is_holiday
FROM (
    SELECT DATE('1900-01-01') + INTERVAL a.NUMBER DAY AS date_val
    FROM (
        SELECT (t4.NUMBER*10000 + t3.NUMBER*1000 + t2.NUMBER*100 + t1.NUMBER*10 + t0.NUMBER) AS NUMBER
        FROM 
            (SELECT 0 AS NUMBER UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t0,
            (SELECT 0 AS NUMBER UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t1,
            (SELECT 0 AS NUMBER UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t2,
            (SELECT 0 AS NUMBER UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t3,
            (SELECT 0 AS NUMBER UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) t4
    ) a
    WHERE DATE('1900-01-01') + INTERVAL a.NUMBER DAY <= '2100-12-31'
) d;

-- Step 3: Load dim_patient
INSERT INTO dim_patient (patient_id, first_name, last_name, date_of_birth, gender, mrn, age_group)
SELECT 
    p.patient_id,
    p.first_name,
    p.last_name,
    p.date_of_birth,
    p.gender,
    p.mrn,
    CASE 
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, '2026-01-05') < 18 THEN '0-18'
        WHEN TIMESTAMPDIFF(YEAR, p.date_of_birth, '2026-01-05') < 65 THEN '19-65'
        ELSE '65+'
    END AS age_group
FROM patients p;

-- Step 4: Load dim_provider
INSERT INTO dim_provider (provider_id, first_name, last_name, credential)
SELECT provider_id, first_name, last_name, credential
FROM providers;

-- Step 5: Load dim_specialty
INSERT INTO dim_specialty (specialty_id, specialty_name, specialty_code)
SELECT specialty_id, specialty_name, specialty_code
FROM specialties;

-- Step 6: Load dim_department
INSERT INTO dim_department (department_id, department_name, floor, capacity)
SELECT department_id, department_name, floor, capacity
FROM departments;

-- Step 7: Load dim_encounter_type
INSERT INTO dim_encounter_type (type_name)
SELECT DISTINCT encounter_type
FROM encounters
WHERE encounter_type IS NOT NULL;

-- Step 8: Load dim_diagnosis
INSERT INTO dim_diagnosis (diagnosis_id, icd10_code, icd10_description)
SELECT diagnosis_id, icd10_code, icd10_description
FROM diagnoses;

-- Step 9: Load dim_procedure
INSERT INTO dim_procedure (procedure_id, cpt_code, cpt_description)
SELECT procedure_id, cpt_code, cpt_description
FROM procedures;

-- Step 10: Load fact_encounters with pre-aggregated metrics
INSERT INTO fact_encounters (
    encounter_id,
    patient_key,
    provider_key,
    specialty_key,
    department_key,
    encounter_type_key,
    encounter_date_key,
    discharge_date_key,
    diagnosis_count,
    procedure_count,
    total_allowed,
    encounter_duration_hours,
    is_readmission
)
SELECT 
    e.encounter_id,
    dp.patient_key,
    dpr.provider_key,
    ds.specialty_key,
    dd.department_key,
    det.encounter_type_key,
    ddate.date_key AS encounter_date_key,
    ddischarge.date_key AS discharge_date_key,
    diag_cnt.cnt AS diagnosis_count,
    proc_cnt.cnt AS procedure_count,
    COALESCE(b.allowed_amount, 0) AS total_allowed,
    TIMESTAMPDIFF(HOUR, e.encounter_date, e.discharge_date) AS encounter_duration_hours,
    IF(
        EXISTS (
            SELECT 1 
            FROM encounters e2 
            WHERE e2.patient_id = e.patient_id
              AND e2.encounter_type = 'Inpatient'
              AND e2.encounter_date BETWEEN e.discharge_date AND DATE_ADD(e.discharge_date, INTERVAL 30 DAY)
              AND e2.encounter_id != e.encounter_id
        ), TRUE, FALSE
    ) AS is_readmission
FROM encounters e
JOIN dim_patient dp ON dp.patient_id = e.patient_id
JOIN dim_provider dpr ON dpr.provider_id = e.provider_id
JOIN providers p ON p.provider_id = e.provider_id
JOIN dim_specialty ds ON ds.specialty_id = p.specialty_id
JOIN dim_department dd ON dd.department_id = e.department_id
JOIN dim_encounter_type det ON det.type_name = e.encounter_type
JOIN dim_date ddate ON ddate.calendar_date = DATE(e.encounter_date)
JOIN dim_date ddischarge ON ddischarge.calendar_date = DATE(e.discharge_date)
LEFT JOIN billing b ON b.encounter_id = e.encounter_id
LEFT JOIN (
    SELECT encounter_id, COUNT(*) AS cnt
    FROM encounter_diagnoses
    GROUP BY encounter_id
) diag_cnt ON diag_cnt.encounter_id = e.encounter_id
LEFT JOIN (
    SELECT encounter_id, COUNT(*) AS cnt
    FROM encounter_procedures
    GROUP BY encounter_id
) proc_cnt ON proc_cnt.encounter_id = e.encounter_id;

-- Step 11: Load bridge_encounter_diagnoses
INSERT INTO bridge_encounter_diagnoses (encounter_key, diagnosis_key, diagnosis_sequence)
SELECT 
    fe.encounter_key,
    dd.diagnosis_key,
    ed.diagnosis_sequence
FROM encounter_diagnoses ed
JOIN fact_encounters fe ON fe.encounter_id = ed.encounter_id
JOIN dim_diagnosis dd ON dd.diagnosis_id = ed.diagnosis_id;

-- Step 12: Load bridge_encounter_procedures
INSERT INTO bridge_encounter_procedures (encounter_key, procedure_key, procedure_date_key)
SELECT 
    fe.encounter_key,
    dp.procedure_key,
    ddate.date_key
FROM encounter_procedures ep
JOIN fact_encounters fe ON fe.encounter_id = ep.encounter_id
JOIN dim_procedure dp ON dp.procedure_id = ep.procedure_id
JOIN dim_date ddate ON ddate.calendar_date = ep.procedure_date;
