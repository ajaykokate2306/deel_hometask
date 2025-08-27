# Deel Take-Home Data Engineering Project

This repository contains 2 solutions. 
    1. Contains all modern ETL tools to generate the desired output
    2. This is a jupyter notebook that performs all requested tasks

# Solution 1

This solution contains a data pipeline built with **AWS S3, Snowflake, dbt, Airflow, Docker, and Slack**.  
It ingests raw financial data, transforms it into analytics-ready models, and triggers alerts for balance anomalies.

---

## 🚀 Pipeline Overview

1. **Data Ingestion**  
   - Upload `.csv` files (invoices, organizations) to AWS S3.  
   - Snowflake pulls raw data from S3 into the `RAW` schema.  

2. **Data Transformation (dbt)**  
   - Staging models: `stg_invoices`, `stg_organizations`.  
   - Fact model: `fct_invoices` (grain: date × organization, with balance calculation).  
   - Dimension model: `dim_organizations`. (grain: organization)
        Enriched with: total invoices, total payments, total invoice amounts.


3. **Alerts (Airflow + Slack)**  
   - Airflow DAG queries latest balances.  
   - Triggers Slack alert if **balance changes > 50% day-over-day**.  
   - Alerts sent to `#balance-change-alert`.  

---

## 🛠️ Tech Stack
- **AWS S3** – Data lake for raw `.csv` files  
- **Snowflake** – Cloud data warehouse  
- **dbt-core** – SQL transformations (ELT)  
- **Docker** – Containerization for Airflow  
- **Airflow** – Workflow orchestration  
- **Slack API** – Real-time notifications  

---




---

## ⚙️ Setup Instructions

### 1. Clone repo
```bash
git clone https://github.com/<your-username>/deel-takehome.git
cd deel-takehome

### 2. setup Airflow
docker-compose up airflow-init
docker-compose up -d


### 3. Configure Airflow connections
Snowflake: Add your account, user, password, warehouse, db, schema.
Slack: Add Webhook URL in Airflow UI (Connections).

### 4. Run DAG
- Start Airflow UI at http://localhost:8080.
- Trigger org_balance_alerts DAG.
- Check balance-change-alert Slack channel for notifications.

📢 Alerts
Example Slack Alert:
    Hello! This is an automated trigger to notify about >50% balance change in the organizations. Please find the details below 
    ALERT: Org -7159892619161641325 balance changed >50% on 2024-04-24: 50097.97 → 0.0 (Δ 100.00%)


Author
- Ajay Kokate
