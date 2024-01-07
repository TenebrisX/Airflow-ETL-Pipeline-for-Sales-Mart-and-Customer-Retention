# Airflow ETL Pipeline for Sales Mart and Customer Retention

## Table of Contents
- [Overview](#overview)
- [File Structure](#file-structure)
- [DAG Description](#dag-description)

## Overview
This repository contains the code for an Apache Airflow ETL pipeline designed to manage the update process for tables in a sales data mart and compute customer retention metrics. The pipeline engages with an external API for report generation and retrieval, handles file uploads from S3, and executes data transformations to update both staging and mart tables.

## File Structure

- `/src/dags`
  - `increment_dag.py`
  - `original_tables_dag.py`
- `/sql`
  - `mart.d_city.sql`
  - `mart.d_customer.sql`
  - `mart.d_item.sql`
  - `mart.f_customer_retention.sql`
  - `mart.f_sales.sql`


## DAG Description
The main Airflow DAG file is `increment_dag.py`. The DAG consists of the following tasks:

1. **generate_report:** Initiates the generation of a report via a POST request and pushes the resulting task_id to XCom.

2. **get_report:** Retrieves a report_id from an API endpoint and pushes it to XCom.

3. **get_increment:** Retrieves an increment_id from an API endpoint and pushes it to XCom.

4. **upload_user_order_log_inc:** Uploads data from an S3 file to a PostgreSQL table in a staging environment for the 'user_order_log' table.

5. **upload_customer_research_inc:** Uploads data from an S3 file to a PostgreSQL table in a staging environment for the 'customer_research' table.

6. **upload_user_activity_log_inc:** Uploads data from an S3 file to a PostgreSQL table in a staging environment for the 'user_activity_log' table.

7. **update_d_city_table:** Executes SQL script for updating the 'd_city' table.

8. **update_d_item_table:** Executes SQL script for updating the 'd_item' table.

9. **update_d_customer_table:** Executes SQL script for updating the 'd_customer' table.

10. **update_f_sales:** Executes SQL script for updating the 'f_sales' table.

11. **update_f_customer_retention:** Executes SQL script for updating the 'f_customer_retention' table.

