# Healthcare Analytics Lab — OLTP to Star Schema

**Status:** Star schema designed, ETL implemented, performance validated on synthetic data

---

## Project overview

This mini-project simulates a real-world data engineering task: migrating analytics workloads from a normalized OLTP schema to a star schema designed for fast, flexible reporting. The OLTP schema mirrors a typical clinical system (EHR-like) and is optimized for transactions. The star schema is optimized for analytics and reporting.

Goals completed so far:

* Analyzed the production-style OLTP schema and identified analytic pain points
* Generated realistic synthetic data and populated the OLTP database for testing
* Designed a star schema (dimensions + encounter fact + bridge tables) at the encounter grain
* Implemented SQL-based ETL to populate dimensions, a fact table with pre-aggregated metrics, and bridge tables
* Validated query correctness and measured performance improvements for representative analytics queries

All data is synthetic and used strictly for educational purposes.

---

## What’s in this repository (high level)

* OLTP schema DDL (relational schema used for the simulated production system)
* Star schema DDL (dimension + fact + bridge definitions)
* Data generation scripts (synthetic data for tables and cardinalities)
* ETL scripts (SQL transformations that load dimensions, the encounter fact, and bridges)
* Example analytics queries and EXPLAIN ANALYZE output for performance comparison
* Documentation: design notes, ETL design, and this README / reflection

---

## OLTP schema summary

The normalized OLTP schema models clinical operations in 3NF. Key tables include:

* `patients` — patient demographic master data
* `providers` — clinicians and their metadata (linked to specialties and departments)
* `encounters` — transactional visits (inpatient / outpatient / ER)
* `diagnoses`, `encounter_diagnoses` — ICD-10 master + encounter bridge
* `procedures`, `encounter_procedures` — CPT master + encounter bridge
* `billing` — claims and allowed amounts

**Design characteristics:** many small, normalized tables; foreign keys enforce integrity; excellent for transactional correctness, but expensive for analytic joins.

---

## Synthetic dataset and cardinality

The test dataset was sized to mimic a small hospital workload:

|                Table | Row count |
| -------------------: | --------: |
|             patients |     2,000 |
|          specialties |        10 |
|            providers |       100 |
|           encounters |    10,000 |
|            diagnoses |        50 |
|  encounter_diagnoses |    30,052 |
|           procedures |        50 |
| encounter_procedures |    14,891 |
|              billing |    10,000 |

These volumes are large enough to show join costs and to validate star schema benefits without requiring a cluster.

---

## Star schema design (summary)

* **Fact**: `fact_encounters` — one row per encounter (encounter grain), stores pre-aggregated metrics such as `diagnosis_count`, `procedure_count`, `total_allowed`, `encounter_duration_hours`, and `is_readmission`. Also stores surrogate foreign keys (patient_key, provider_key, specialty_key, date keys).
* **Dimensions**: `dim_date`, `dim_patient`, `dim_provider`, `dim_specialty`, `dim_department`, `dim_diagnosis`, `dim_procedure`, `dim_encounter_type`
* **Bridges**: `bridge_encounter_diagnoses`, `bridge_encounter_procedures` to model many-to-many relationships without changing fact grain

**Why this layout:** preserves the encounter grain for facts, avoids duplicating fact rows for each diagnosis/procedure, and stores precomputed metrics so common analytic queries do minimal work at query time.

---

## ETL approach (summary, non-code)

**Dimension loads**

* Load `dim_date` once (calendar table covering the range used).
* Load each dimension from the OLTP master tables (patients, providers, specialties, etc.), creating surrogate keys and preserving source IDs for traceability.
* Use `age_group` derivation and light transformations where useful.

**Fact load**

* For each encounter in OLTP:

  * Resolve dimension surrogate keys by joining on source IDs (map patient_id → patient_key, provider_id → provider_key, etc.).
  * Precompute metrics:

    * `diagnosis_count` and `procedure_count` via aggregated counts from encounter bridge tables
    * `total_allowed` as the sum of allowed_amount from billing per encounter
    * `encounter_duration_hours` from discharge_date − encounter_date
    * `is_readmission` computed at ETL by checking for an inpatient return within 30 days **and matching specialty** (the ETL expression mirrors the RDBMS readmission query to ensure identical results)
  * Insert one row per encounter into `fact_encounters`.

**Bridges**

* Populate `bridge_encounter_diagnoses` and `bridge_encounter_procedures` by joining OLTP bridge tables to the loaded fact and dimensions. Aggregate or deduplicate at load-time so each `(encounter_key, diagnosis_key)` and `(encounter_key, procedure_key)` pair is unique.

**Missing data**

* Use COALESCE/defaults for NULLs in numeric metrics (e.g., `total_allowed = 0`).
* Document and flag encounters with missing discharge dates or other required fields.

**Refresh strategy**

* The delivered ETL is written as a full-refresh workflow (truncate + reload) for clarity and reproducibility.
* For production readiness, switch to incremental loads:

  * Load new/changed dimension rows with SCD logic (Type 1/Type 2 as required).
  * Insert new encounters incrementally and update affected aggregates (or refresh downstream aggregates).
  * Handle late-arriving facts with upsert logic and, if necessary, backfill recalculation for derived fields (e.g., readmission flags).
* The repository includes notes on how to convert the full-refresh SQL into incremental steps.

---

## Validation & correctness

* The ETL logic for `is_readmission` was carefully aligned with the RDBMS definition (patient-level, inpatient only, 30-day window, and requiring the readmission be within the same specialty). This ensures parity between the OLTP-derived reports and the star schema results.
* Bridge table inserts use GROUP BY / DISTINCT patterns to avoid duplicate primary-key violations.

---

## Performance quantification (representative queries)

All measurements were taken on the synthetic dataset using local MySQL (EXPLAIN ANALYZE outputs and measured execution times). Times are representative for the dataset sizes above.

### Query 1 — Total allowed by month and specialty

|      Schema |                        Execution time |
| ----------: | ------------------------------------: |
|       RDBMS |                               60.5 ms |
|        Star |                               33.9 ms |
| Improvement | **60.5 / 33.9 ≈ 1.78× faster** (star) |

**Why:** the star schema stores pre-aggregated financial metrics at the fact level and uses surrogate keys, so the query avoids repeated joins and aggregates over a smaller effective row set.

### Query 2 — 30-day readmission rate by specialty

|      Schema |                        Execution time |
| ----------: | ------------------------------------: |
|       RDBMS |                               44.6 ms |
|        Star |                               13.7 ms |
| Improvement | **44.6 / 13.7 ≈ 3.26× faster** (star) |

**Why:** the star schema precomputes the readmission flag during ETL and performs aggregations over the fact table. This eliminates expensive self-joins and date-range checks at query time.

---

## Trade-offs & reflection

**Gains**

* Significant query acceleration for analytics queries (1.8× to 3.3× in representative tests).
* Much simpler SQL for business users — most analytics are a join of fact to a couple of dimensions and straight aggregations.
* Deterministic, single-source metrics (pre-aggregated values computed once in ETL).

**Costs**

* Added ETL complexity — logic and orchestration are required to map natural keys to surrogate keys, deduplicate, handle SCDs, and compute derived metrics.
* Some data duplication (storing keys and pre-aggregates in the fact).
* Need for a refresh strategy and processes to handle late-arriving data and dimension history.

**Verdict**

* For analytics workloads the trade-off is justified: faster queries and simpler analytics outweigh the storage and ETL complexity for typical reporting and BI use cases.

---

## How to reproduce 

1. Create the OLTP schema and load synthetic data (run the RDBMS schema and load scripts).
2. Create the star schema (dimension and fact DDL).
3. Run the ETL scripts in order: load dimensions, load fact table, load bridge tables. The ETL scripts are written to be run on a local MySQL instance and include notes for disabling/enabling foreign key checks during full refreshes.
4. Run the example analytics queries (both RDBMS and star variants) and compare EXPLAIN ANALYZE outputs and run times.

---

## Limitations and next steps

* Consider adding a dedicated billing fact table if detailed billing analytics are required independently from encounter-level metrics.
* Add automated tests and row-count reconciliations between source and target to guarantee ETL correctness on incremental runs.
* Add pre-materialized monthly summary tables (snapshots) for very large-scale data or frequent dashboard queries.

---

## License & acknowledgments

This repository contains educational/synthetic data and is intended for learning and demonstration purposes only.


