# **Part 4: Analysis & Reflection**

## **Why Is the Star Schema Faster?**

The star schema is faster primarily because it is designed for analytical workloads. In our normalized RDBMS schema, queries required multiple joins across normalized tables, often using natural keys. For example, to compute total encounters by specialty and month, we had to join `encounters , providers , specialties , billing , dates`, which created a lot of join overhead.

In the star schema, most of these joins are replaced by surrogate keys in the fact table. The fact table already contains the `patient_key`, `provider_key`, `specialty_key`, and `date_key`, so the database only needs to join the fact to a few small dimension tables. Additionally, metrics like `diagnosis_count`, `procedure_count`, and `total_allowed` are pre-computed during ETL and stored in the fact table. This **pre-aggregation eliminates the need to calculate counts or sums at query time**, drastically reducing execution time.

Denormalization helps analytical queries because it minimizes the number of joins and allows queries to scan fewer rows. Aggregations, grouping, and filtering can be done mostly on the fact table, which is indexed by surrogate keys for fast lookups.

---

## **Trade-offs: What Did We Gain? What Did We Lose?**

### **What We Gave Up**

* **Data duplication:** Storing foreign keys and pre-aggregated metrics in the fact table duplicates some information from dimension tables.
* **ETL complexity:** Loading the star schema required careful dimension lookups, bridge table creation, and handling of pre-aggregated metrics. Updates and late-arriving data are harder to manage.

### **What We Gained**

* **Faster queries:** Analytical queries that took 50–300 milliseconds in RDBMS dropped to 30–140 milliseconds in the star schema.
* **Simpler analysis:** Querying aggregated metrics (like total allowed by month or readmission rates) is much simpler since most computations are pre-calculated.

Overall, the trade-off was worth it. In analytical workloads, the speed and simplicity of the star schema significantly outweigh the costs of ETL complexity and storage overhead.

---

## **Bridge Tables: Worth It?**

We kept `bridge_encounter_diagnoses` and `bridge_encounter_procedures` instead of denormalizing everything into the fact table.

**Reasons:**

* Many-to-many relationships exist between encounters and diagnoses/procedures. Denormalizing would require repeating fact rows for each diagnosis or procedure, inflating the fact table dramatically.
* Bridge tables allow efficient querying of the associations without duplicating large amounts of fact data.

**Trade-off:**

* Extra join is required when analyzing diagnoses or procedures, but the impact is small because bridge tables are indexed on surrogate keys.

In production, I would likely keep bridge tables for many-to-many relationships. If performance became critical for very high-frequency queries, we could consider pre-materialized views or aggregations to speed up common queries.

---

## **Performance Quantification**

**Query 1: Total Allowed by Month and Specialty**

| Schema      | Execution Time            |
| ----------- | ------------------------- |
| RDBMS       | 60.5 ms                   |
| Star        | 33.9 ms                   |
| Improvement | 40.7 / 33.9 ≈ 1.78x faster |

**Main reason for speedup:** Pre-aggregated metrics in the fact table and fewer joins due to surrogate keys.

**Query 2: Readmission Rate by Specialty**

| Schema      | Execution Time          |
| ----------- | ----------------------- |
| RDBMS       | 44.6 ms                 |
| Star        | 13.7 ms                 |
| Improvement | 44.6 / 13.7 ≈ 3.22x faster |

**Main reason for speedup:** Pre-computation of the `is_readmission` flag during ETL and smaller join footprint due to denormalized design.

---

**Conclusion:**

The star schema provides a clear performance advantage for analytical queries due to pre-aggregation, surrogate keys, and simplified joins. While it increases ETL complexity and storage requirements, the benefits in query speed and analytical simplicity are substantial. Keeping bridge tables for many-to-many relationships is a reasonable trade-off, balancing normalization and performance. Overall, for this project, the star schema design was highly effective and justified.


