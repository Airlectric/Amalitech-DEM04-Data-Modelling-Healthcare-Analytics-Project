# Healthcare Analytics Lab: OLTP to Star Schema

## Project Overview

This mini project simulates a **real-world data engineering scenario** in a healthcare analytics environment. I am acting as a junior data engineer at *HealthTech Analytics*, where the production system uses a **normalized OLTP (3NF) database** to support clinical operations. While suitable for transactions, this design is inefficient for analytics and reporting.

The goal of the project is to:

1. Analyze the existing OLTP schema
2. Identify why analytical queries are slow and complex
3. Design and later build an **optimized star schema** for analytics workloads

This README documents **what has been completed so far**: schema analysis and realistic data generation for the OLTP system.

---

## Part 1: Normalized OLTP Schema

The production database is designed in **Third Normal Form (3NF)** to avoid redundancy and ensure data integrity. It consists of **9 interrelated tables**, modeling core healthcare entities such as patients, providers, encounters, diagnoses, procedures, and billing.

### Key Characteristics of the OLTP Design

* Highly normalized tables
* Strong use of foreign keys
* Optimized for inserts and updates
* Complex joins required for analytics

This structure mirrors real clinical systems such as EHRs.

---

## OLTP Tables and Purpose

### Patients

Stores demographic information for each patient.

### Specialties

Lookup table defining medical specialties (e.g., Cardiology, Orthopedics).

### Departments

Represents hospital departments, including location and capacity.

### Providers

Healthcare professionals linked to specialties and departments.

### Encounters

Represents patient visits (Outpatient, Inpatient, ER).
Acts as the **core transactional table** connecting patients, providers, and departments.

### Diagnoses

Master list of ICD-10 diagnosis codes.

### Encounter_Diagnoses

Bridge table supporting the **many-to-many** relationship between encounters and diagnoses.
Includes diagnosis sequence (primary, secondary, etc.).

### Procedures

Master list of CPT procedure codes.

### Encounter_Procedures

Bridge table capturing procedures performed during encounters.

### Billing

Financial data related to encounters, including claim and allowed amounts.

---

## Data Generation and Population

To simulate a realistic healthcare workload, synthetic data was generated and inserted into all tables. The data volumes were intentionally chosen to reflect **realistic cardinalities and relationships**.

### Current Row Counts

| Table                | Row Count |
| -------------------- | --------- |
| patients             | 2,000     |
| specialties          | 10        |
| providers            | 100       |
| encounters           | 10,000    |
| diagnoses            | 50        |
| encounter_diagnoses  | 30,052    |
| procedures           | 50        |
| encounter_procedures | 14,891    |
| billing              | 10,000    |

### Data Characteristics

* Each patient has multiple encounters
* Each encounter can have multiple diagnoses and procedures
* Diagnosis and procedure counts vary per encounter
* Billing data exists for every encounter

This results in **join-heavy analytical queries**, which is intentional for demonstrating OLTP limitations.

---

## Why This OLTP Design Is Poor for Analytics

Although well-designed for transactions, this schema presents challenges for analytics:

* Queries require **6–9 table joins**
* Aggregations are expensive
* Repeated joins across large bridge tables
* Difficult to support BI tools efficiently

These issues motivate the transition to a **star schema**.

---

## Next Steps (Planned)

The next phases of the project will include:

1. Identify fact tables and dimensions
2. Design a healthcare-focused star schema
3. Create dimension tables (Patient, Provider, Date, Diagnosis, Procedure, Department)
4. Build a central fact table for encounters/billing
5. Write ETL SQL to transform OLTP using star schema
6. Demonstrate simplified and faster analytical queries

---

## Notes

This project intentionally mirrors real enterprise healthcare data systems and analytics migration patterns. All data is synthetic and used solely for educational purposes.
