Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

# IDEA Education Analytics Platform (dbt + Databricks)

## Overview
This project implements an end-to-end analytics data pipeline using dbt and Databricks. It follows a medallion architecture (Bronze → Silver → Intermediate → Gold) and builds a star schema to support education analytics use cases such as student performance, attendance tracking, and enrollment analysis.

---

## 🏗Architecture

Data flows through layered transformations:

Bronze → Raw ingestion layer  
Silver → Cleaned and standardized data  
Intermediate → Business logic transformations  
Snapshots → SCD Type 2 history tracking  
Gold → Star schema (Facts + Dimensions)

---

##  Star Schema Design

### Dimensions:
- dim_students (SCD Type 2)
- dim_schools
- dim_date

### Facts:
- fact_attendance
- fact_assessments
- fact_enrollment

---

## Business KPIs

Built on top of the star schema:

- Student Attendance Rate
- School Pass Rate
- Average Score by Subject
- Active Student Enrollment

---

## Tech Stack

- dbt (data transformation)
- Databricks (Delta Lake)
- SQL
- Incremental models (MERGE strategy)
- SCD Type 2 snapshots

---

##  How to Run

Run the full IDEA pipeline:

```bash
dbt build --select tag:idea