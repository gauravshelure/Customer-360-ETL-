# Customer 360 ETL Data Warehouse Project

## Project Overview
This project demonstrates an end-to-end ETL pipeline that integrates customer data from multiple sources such as website data, CRM systems, and sales transactions.  

The pipeline extracts data from different formats (CSV and JSON), performs data cleaning and transformation using Python, loads the processed data into a MySQL data warehouse, and visualizes insights using Power BI.

The goal is to create a unified **Customer 360 View** that helps businesses analyze customer behavior and spending patterns.

---

## Tools & Technologies
- Python (Pandas)
- MySQL
- SQL
- Power BI
- Git 

---

## Project Architecture

Source Data → Staging Tables → Python Transformation → Data Warehouse → Power BI Dashboard

Data Sources:
- Website customer data (CSV)
- Sales transactions (CSV)
- CRM membership data (JSON)

---

## ETL Workflow

### 1. Extract
Customer data is collected from multiple sources:
- Website data (CSV file)
- Sales transaction data (CSV file)
- CRM membership data (JSON file)

### 2. Load to Staging
Raw data is loaded into staging tables in MySQL.

Tables:
- website_staging
- sales_staging
- crm_staging

### 3. Transform
Data transformation is performed using Python:
- Remove duplicate customers
- Standardize customer names
- Merge datasets
- Calculate customer total spending

### 4. Load to Data Warehouse
Transformed data is loaded into the final table:

customer_360

Columns:
- customer_id
- name
- email
- city
- membership
- total_spent
