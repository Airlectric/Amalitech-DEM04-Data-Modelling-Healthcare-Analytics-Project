-- QUESTION 1: Monthly Encounters by Specialty
SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    e.encounter_type,
    COUNT(DISTINCT e.encounter_id) AS total_encounters,
    COUNT(DISTINCT e.patient_id) AS unique_patients
FROM 
    encounters e
JOIN 
    providers p ON e.provider_id = p.provider_id
JOIN 
    specialties s ON p.specialty_id = s.specialty_id
GROUP BY 
    month, s.specialty_name, e.encounter_type
ORDER BY 
    month, s.specialty_name, e.encounter_type;



-- QUESTION 2: Top Diagnosis-Procedure Pairs
SELECT 
    d.icd10_code,
    pr.cpt_code,
    COUNT(DISTINCT ed.encounter_id) AS encounter_count
FROM 
    encounter_diagnoses ed
JOIN 
    diagnoses d ON ed.diagnosis_id = d.diagnosis_id
JOIN 
    encounter_procedures ep ON ed.encounter_id = ep.encounter_id
JOIN 
    procedures pr ON ep.procedure_id = pr.procedure_id
GROUP BY 
    d.icd10_code, pr.cpt_code
ORDER BY 
    encounter_count DESC
LIMIT 10;


-- QUESTION 3: 30-Day Readmission Rate
SELECT 
    s.specialty_name,
    COUNT(DISTINCT CASE WHEN e2.encounter_id IS NOT NULL THEN e1.encounter_id END) / COUNT(DISTINCT e1.encounter_id) AS readmission_rate
FROM 
    encounters e1
LEFT JOIN 
    encounters e2 ON e1.patient_id = e2.patient_id 
                  AND e2.encounter_date BETWEEN e1.discharge_date AND DATE_ADD(e1.discharge_date, INTERVAL 30 DAY)
                  AND e1.encounter_type = 'Inpatient'
                  AND e2.encounter_type = 'Inpatient'
                  AND e1.encounter_id != e2.encounter_id
JOIN 
    providers p ON e1.provider_id = p.provider_id
JOIN 
    specialties s ON p.specialty_id = s.specialty_id
WHERE 
    e1.encounter_type = 'Inpatient'
GROUP BY 
    s.specialty_name
ORDER BY 
    readmission_rate DESC;


-- QUESTION 4: Revenue by Specialty & Month

SELECT 
    DATE_FORMAT(e.encounter_date, '%Y-%m') AS month,
    s.specialty_name,
    SUM(encounter_total.total_allowed) AS total_allowed
FROM (
    -- Step 1: Pre-aggregate billing per encounter
    SELECT encounter_id, SUM(allowed_amount) AS total_allowed
    FROM billing
    GROUP BY encounter_id
) AS encounter_total
JOIN encounters e ON e.encounter_id = encounter_total.encounter_id
JOIN providers p ON e.provider_id = p.provider_id
JOIN specialties s ON p.specialty_id = s.specialty_id
GROUP BY month, s.specialty_name
ORDER BY month, total_allowed DESC;
