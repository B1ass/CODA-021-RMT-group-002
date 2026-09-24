🚀 Hacktiv8 - Final Project

**by Selvyana Natalia Purnomo, Joshua Andika, Muhammad Rifqi Sahib, Kairenzo Vemil**  
**CODA | Batch: RMT-021**

## 📌 Project Overview

This project performs the **Extract, Transform, and Load (ETL) process** using **Apache Airflow**. The pipeline extracts global black money transaction data, cleans and transforms it, then loads the processed data into **PostgreSQL**. The data is then visualized in **Tableau** to generate insights into:

📊 **Money laundering risk score distribution**  
🏢 **Shell company involvement**  
🌐 **Tax haven country activity and cross-border transaction routes**  
📈 **Risk score patterns across industries and transaction groups**  

These insights aim to provide a better understanding of transaction risk patterns and support risk-based monitoring and further investigation of potentially suspicious transactions.

## 📖 Background

Based on existing research, the growing complexity of global financial transactions makes high-risk activities increasingly difficult to detect. Black money refers to hidden or untaxed funds that may flow through tax havens, jurisdictions often associated with low tax rates and financial secrecy. Shell companies and tax havens are important indicators in Anti-Money Laundering (AML) monitoring.

To better understand these patterns, this project analyzes **the Global Black Money Transactions Dataset, focusing on Risk Score, shell company involvement, tax haven activity, industry differences, reporting status, and cross-border transaction routes**. The analysis aims to provide data-driven insights into transaction risk patterns and support a more structured understanding of AML-related indicators.

## 🎯 Objectives 

As Data Analysts, our goal is to prepare a **comprehensive report** that provides insights into global black money transaction patterns, focusing on:

✔️ **Risk Score, shell company involvement, and tax haven distribution**  
✔️ **Money laundering risk across industries**  
✔️ **Risk Score differences by source of money**  
✔️ **Transaction amount differences by reporting status**  
✔️ **Cross-border transaction route patterns**  

Additionally, we aim to provide **data-driven insights and practical recommendations** based on the observed patterns and statistical findings to **support a more structured understanding of transaction risk and Anti-Money Laundering (AML) monitoring**.

## 📂 Dataset Source

📌 [Dataset Link](https://www.kaggle.com/datasets/waqi786/global-black-money-transactions-dataset/)

## ⚙️ Data Analysis and ETL Process (Airflow DAG)

This project **processes and analyzes global black money transaction data through an ETL workflow orchestrated using Apache Airflow**. The data is **extracted and processed using Python, validated for data quality, and stored in PostgreSQL**. The analyzed data is then **visualized in Tableau** to explore transaction patterns and money laundering risk indicators.

🔹 **Step 1: Extract & Transform** – Uses Python to load, clean, and prepare transaction data for analysis.  
🔹 **Step 2: Data Validation** – Applies Great Expectations to validate data quality and ensure data consistency.  
🔹 **Step 3: Load** – Stores the processed data in PostgreSQL for further analysis.  
🔹 **Step 4: Workflow Orchestration** – Uses Apache Airflow to manage and orchestrate the ETL workflow.  
🔹 **Step 5: Data Visualization** – Presents analytical findings through Tableau dashboards.

## 🏗️ Technology Stack

✅ Python – Handles data processing, cleaning, and analysis.  
✅ Docker – Provides a containerized environment for project execution.  
✅ Apache Airflow – Orchestrates and manages the ETL workflow.  
✅ PostgreSQL – Stores the processed transaction data.  
✅ Great Expectations – Validates data quality and consistency.  
✅ Tableau – Creates interactive dashboards to visualize analytical findings.  

## 📌 Conclusion and Business Impact

✅ Automated ETL workflow using Apache Airflow to streamline data processing and integration.  
✅ Reliable data quality through Great Expectations validation before loading data into PostgreSQL.  
✅ Data-driven insights into global black money transaction patterns, Risk Scores, shell company involvement, and tax haven activity.  
✅ Statistical findings providing a clearer understanding of risk patterns across transaction groups and cross-border routes.  
✅ AML monitoring insights supporting more structured risk assessment and further investigation of potentially unusual transaction patterns.  

## ⛓️‍💥 Let's Connect!

💼 **Selvyana Natalia Purnomo** - [Linkedin](https://www.linkedin.com/in/selvyanatalia)  
💼 **Joshua Andika** - [Linkedin](https://www.linkedin.com/in/joshua-andika)  
💼 **Muhammad Rifqi Sahib** - [Linkedin](https://www.linkedin.com/in/rifqi-sahib)  
💼 **Kairenzo Vemil** - [Linkedin](https://linkedin.com/in/kairenzo-vemil-51027b36a)

## File Explanation
- Final_Project_Kelompok_2.ipynb : was file for data analysis
- script/extract.py : was file for extracting dataset
- script/transform.py : was file for transforming raw dataset
- script/load.py : was file for load the transform dataset into postgresSQL
- script/main.py : was file for organize the route for the airflow job
- script/validate.py : was file for great expectation validation
- script/test.py : both file was testing the file from local running without disturb the pipeline flow
- dags/big_black_money_dag.py : was file for managing the job for the airflow
