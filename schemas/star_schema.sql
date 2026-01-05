CREATE DATABASE IF NOT EXISTS hospital_star_db;
USE hospital_star_db;

-- Dimension Tables

-- dim_date: For time-based analysis
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY AUTO_INCREMENT,
    calendar_date DATE NOT NULL,
    year INT,
    month INT,
    quarter INT,
    day_of_week INT,
    is_holiday BOOLEAN DEFAULT FALSE,
    -- Comment: Pre-populated calendar for efficient date joins
    INDEX idx_calendar_date (calendar_date)
);

-- dim_patient: Patient demographics
CREATE TABLE dim_patient (
    patient_key INT PRIMARY KEY AUTO_INCREMENT,
    patient_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    date_of_birth DATE,
    gender CHAR(1),
    mrn VARCHAR(20) UNIQUE,
    age_group VARCHAR(20),  -- Derived: '0-18', etc.
    -- Comment: Denormalized patient info for quick access
    UNIQUE KEY uk_patient_id (patient_id)
);

-- dim_specialty: Specialty lookup
CREATE TABLE dim_specialty (
    specialty_key INT PRIMARY KEY AUTO_INCREMENT,
    specialty_id INT NOT NULL,
    specialty_name VARCHAR(100),
    specialty_code VARCHAR(10),
    -- Comment: Simple lookup for aggregation
    UNIQUE KEY uk_specialty_id (specialty_id)
);

-- dim_department: Department lookup
CREATE TABLE dim_department (
    department_key INT PRIMARY KEY AUTO_INCREMENT,
    department_id INT NOT NULL,
    department_name VARCHAR(100),
    floor INT,
    capacity INT,
    -- Comment: For location-based queries
    UNIQUE KEY uk_department_id (department_id)
);

-- dim_provider: Provider details (denormalized from OLTP)
CREATE TABLE dim_provider (
    provider_key INT PRIMARY KEY AUTO_INCREMENT,
    provider_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    credential VARCHAR(20),
    specialty_key INT,
    department_key INT,
    UNIQUE KEY uk_provider_id (provider_id),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key)
);

-- dim_encounter_type: Encounter type lookup
CREATE TABLE dim_encounter_type (
    encounter_type_key INT PRIMARY KEY AUTO_INCREMENT,
    type_name VARCHAR(50) UNIQUE,
    -- Comment: Categorical dimension for filtering
    UNIQUE KEY uk_type_name (type_name)
);

-- dim_diagnosis: Diagnosis lookup
CREATE TABLE dim_diagnosis (
    diagnosis_key INT PRIMARY KEY AUTO_INCREMENT,
    diagnosis_id INT NOT NULL,
    icd10_code VARCHAR(10),
    icd10_description VARCHAR(200),
    UNIQUE KEY uk_diagnosis_id (diagnosis_id)
);

-- dim_procedure: Procedure lookup
CREATE TABLE dim_procedure (
    procedure_key INT PRIMARY KEY AUTO_INCREMENT,
    procedure_id INT NOT NULL,
    cpt_code VARCHAR(10),
    cpt_description VARCHAR(200),
    UNIQUE KEY uk_procedure_id (procedure_id)
);

-- Fact Table
CREATE TABLE fact_encounters (
    encounter_key INT PRIMARY KEY AUTO_INCREMENT,
    encounter_id INT NOT NULL,  -- Original ID for reference
    patient_key INT,
    provider_key INT,
    specialty_key INT,  -- Denormalized for quick access
    department_key INT, -- Denormalized for quick access
    encounter_type_key INT,
    encounter_date_key INT,  -- FK to dim_date
    discharge_date_key INT,  -- FK to dim_date
    diagnosis_count INT,  -- Pre-agg
    procedure_count INT,  -- Pre-agg
    total_allowed DECIMAL(12,2),  -- Pre-agg from billing
    encounter_duration_hours INT,  -- Pre-computed
    is_readmission BOOLEAN DEFAULT FALSE,  -- Flag for 30-day readmission
    FOREIGN KEY (patient_key) REFERENCES dim_patient(patient_key),
    FOREIGN KEY (provider_key) REFERENCES dim_provider(provider_key),
    FOREIGN KEY (specialty_key) REFERENCES dim_specialty(specialty_key),
    FOREIGN KEY (department_key) REFERENCES dim_department(department_key),
    FOREIGN KEY (encounter_type_key) REFERENCES dim_encounter_type(encounter_type_key),
    FOREIGN KEY (encounter_date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (discharge_date_key) REFERENCES dim_date(date_key),
    -- Comment: Central fact at encounter grain with pre-agg metrics
    INDEX idx_encounter_date_key (encounter_date_key),
    INDEX idx_specialty_key (specialty_key),
    UNIQUE KEY uk_encounter_id (encounter_id)
);

-- Bridge Tables for M:N
CREATE TABLE bridge_encounter_diagnoses (
    encounter_key INT,
    diagnosis_key INT,
    diagnosis_sequence INT,
    PRIMARY KEY (encounter_key, diagnosis_key),
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (diagnosis_key) REFERENCES dim_diagnosis(diagnosis_key),
    -- Comment: Handles multiple diagnoses per encounter
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_diagnosis_key (diagnosis_key)
);

CREATE TABLE bridge_encounter_procedures (
    encounter_key INT,
    procedure_key INT,
    procedure_date_key INT,  -- FK to dim_date
    PRIMARY KEY (encounter_key, procedure_key),
    FOREIGN KEY (encounter_key) REFERENCES fact_encounters(encounter_key),
    FOREIGN KEY (procedure_key) REFERENCES dim_procedure(procedure_key),
    FOREIGN KEY (procedure_date_key) REFERENCES dim_date(date_key),
    -- Comment: Handles multiple procedures per encounter
    INDEX idx_encounter_key (encounter_key),
    INDEX idx_procedure_key (procedure_key)
);