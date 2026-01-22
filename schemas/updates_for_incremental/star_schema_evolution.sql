---- Schema Evolution Scripts ----

-- Schema Alteration: Adding Billing Info to Fact Table
ALTER TABLE hospital_star_db.fact_encounters 
ADD COLUMN claim_date_key INT NULL AFTER discharge_date_key,
ADD COLUMN claim_amount DECIMAL(12,2) NULL AFTER total_allowed,
ADD INDEX idx_claim_date_key (claim_date_key);

-- Alter dim_patient for SCD Type 2
ALTER TABLE hospital_star_db.dim_patient
ADD COLUMN start_date DATE DEFAULT NULL,
ADD COLUMN end_date DATE DEFAULT '9999-12-31',
ADD COLUMN is_current BOOLEAN DEFAULT TRUE,
DROP UNIQUE KEY uk_patient_id,  -- Removed as SCD2 allows multiples
ADD UNIQUE KEY uk_patient_id_current (patient_id, is_current);  -- Unique only for current

-- Alter fact_encounters
ALTER TABLE hospital_star_db.fact_encounters
ADD COLUMN load_date DATE DEFAULT NULL;


-- ETL Control Table to track loads

CREATE TABLE IF NOT EXISTS hospital_star_db.etl_control (
    control_id       INT AUTO_INCREMENT PRIMARY KEY,
    load_type        VARCHAR(20),          -- 'FULL' or 'INCREMENTAL'
    last_watermark   TIMESTAMP NULL,       -- new: remembers the cutoff for next run
    load_date        DATETIME,             -- when this run started
    records_processed INT DEFAULT 0,
    status           VARCHAR(20),          -- 'SUCCESS', 'FAILURE', 'RUNNING', etc.
    error_message    TEXT
);