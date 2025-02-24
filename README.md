# Data Warehouse and Analytics Project!
Welcome to the Data Warehouse and Analytics Project repository!
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. This portfolio project highlights industry best practices in data engineering and analytics.

# Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
<img width="682" alt="image" src="https://github.com/user-attachments/assets/d3ea30d1-5a00-4755-8c5b-8ff619c89f2b" />

1. Bronze Layer: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. Silver Layer: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. Gold Layer: Houses business-ready data modeled into a star schema required for reporting and analytics.

# Project Overview
This project involves:

1. Data Architecture: Designing a Modern Data Warehouse Using Medallion Architecture Bronze, Silver, and Gold layers.
2. ETL Pipelines: Extracting, transforming, and loading data from source systems into the warehouse.
3. Data Modeling: Developing fact and dimension tables optimized for analytical queries.
4. Analytics & Reporting: Creating SQL-based reports and dashboards for actionable insights.

# Project Requirments
# Building the Data Warehouse
Obejective:

Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

Specifications:

1. Data Sources: Import data from two source systems (ERP and CRM) provided as CSV files.
2. Data Quality: Cleanse and resolve data quality issues prior to analysis.
3. Integration: Combine both sources into a single, user-friendly data model designed for analytical queries.
4. Scope: Focus on the latest dataset only; historization of data is not required.
5. Documentation: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

# BI: Analytics & Reporting
Objective:

Develop SQL-based analytics to deliver detailed insights into:

1. Customer Behavior
2. Product Performance
3. Sales Trends
