-- Question 1: Monthly Encounters by Specialty

SELECT 
    CONCAT(dd.year, '-', LPAD(dd.month, 2, '0')) AS month,
    ds.specialty_name,
    det.type_name AS encounter_type,
    COUNT(*) AS total_encounters,
    COUNT(DISTINCT fe.patient_key) AS unique_patients
FROM fact_encounters fe
JOIN dim_date dd ON fe.encounter_date_key = dd.date_key
JOIN dim_specialty ds ON fe.specialty_key = ds.specialty_key
JOIN dim_encounter_type det ON fe.encounter_type_key = det.encounter_type_key
GROUP BY 
    dd.year,
    dd.month,
    ds.specialty_name,
    det.type_name
ORDER BY 
    dd.year,
    dd.month,
    ds.specialty_name,
    det.type_name;


-- Question 2: Top Diagnosis-Procedure Pairs

SELECT 
    dd.icd10_code,
    dp.cpt_code,
    COUNT(DISTINCT fe.encounter_key) AS encounter_count
FROM 
    fact_encounters fe
JOIN 
    bridge_encounter_diagnoses bed ON fe.encounter_key = bed.encounter_key
JOIN 
    dim_diagnosis dd ON bed.diagnosis_key = dd.diagnosis_key
JOIN 
    bridge_encounter_procedures bep ON fe.encounter_key = bep.encounter_key
JOIN 
    dim_procedure dp ON bep.procedure_key = dp.procedure_key
GROUP BY 
    dd.icd10_code, dp.cpt_code
ORDER BY encounter_count DESC
LIMIT 10;


-- Question 3: 30-Day Readmission Rate
SELECT 
    ds.specialty_name,
    SUM(fe.is_readmission) / COUNT(*) AS readmission_rate
FROM 
    fact_encounters fe
JOIN 
    dim_specialty ds ON fe.specialty_key = ds.specialty_key
JOIN 
    dim_encounter_type det ON fe.encounter_type_key = det.encounter_type_key
WHERE 
    det.type_name = 'Inpatient'
GROUP BY 
    ds.specialty_name
ORDER BY readmission_rate DESC;


--Question 4: Revenue by Specialty & Month

SELECT 
    CONCAT(dd.year, '-', dd.month) AS month,
    ds.specialty_name,
    SUM(fe.total_allowed) AS total_allowed
FROM 
    fact_encounters fe
JOIN 
    dim_date dd ON fe.encounter_date_key = dd.date_key
JOIN 
    dim_specialty ds ON fe.specialty_key = ds.specialty_key
GROUP BY 
	dd.month,
    dd.year,
    ds.specialty_name
ORDER BY month, total_allowed DESC;
