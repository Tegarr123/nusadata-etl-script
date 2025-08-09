# Google Sheets ETL Automation Pipeline 📈
A Python-based ETL pipeline for consolidating data from multiple Google Sheets into a central data warehouse sheet. This project automates the process of merging, transforming, and loading data, featuring built-in support for Change Data Capture and Slowly Changing Dimension (SCD) Type 1 updates.

### 📋 Table of Contents
- [Overview](#-overview)
- [Features](#-features)
- [Pipeline Architecture](#-pipeline-architecture)
- [How It Works](#-how-it-works)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Setup & Installation](#-setup--installation)
- [Configuration](#-configuration)
- [Usage](#-usage)

### 🔎 Overview
This project is designed to solve the common challenge of aggregating cleaned, but separate, datasets residing in Google Sheets. It provides a robust, re-runnable script that intelligently merges new and updated records into a master Google Sheet, which acts as a simple data warehouse. The core logic handles updates by overwriting existing records (SCD Type 1) and appends new records, ensuring the data warehouse stays current with minimal manual intervention.

### ✨ Features
- **Automated ETL**: Fully automates the Extract, Transform, and Load process.
- **Google API Integration**: Seamlessly connects to Google Sheets and Google Drive using their respective APIs.
- **Data Consolidation**: Merges data from multiple source Google Sheets into one destination sheet.
- **Change Data Capture (CDC)**: Automatically identifies and processes new rows from the source data.
- **SCD Type 1 Handling**: Updates existing records by overwriting them with the latest data, ensuring no historical versions are kept.
- **Idempotent Design**: The script can be run multiple times without causing data duplication or errors. Anyone can run it to fetch the latest updates.
### 📄 Pipeline Architecture



### ⚙️ How It Works
The ETL process follows these logical steps:
1. **Extract**: The script authenticates with the Google API and reads all data from the specified source Google Sheets into Pandas DataFrames.
2. **Transform**: It merges these DataFrames into a single, consolidated DataFrame.
3. **Load & Update Logic**:
   - The script fetches the current data from the destination "Data Warehouse" sheet.
   - It uses a **primary key** (e.g., an 'ID' column) to differentiate records.
   - **For New Data (CDC)**: Any row from the source data whose primary key does not exist in the warehouse is identified as a new record and is appended to the warehouse.
   - **For Existing Data (SCD Type 1)**: Any row from the source data whose primary key already exists in the warehouse is identified as an update. The script then overwrites the existing row in the warehouse with the new data.
4. **Final Load**: The updated DataFrame (containing both new and modified rows) is written back to the destination Google Sheet.

### 📁 Project Structure


### 🔧 Prerequisites
- Python 3.8+
- A Google Cloud Platform (GCP) project.
- Enabled Google Drive API and Google Sheets API in your GCP project.
- A Google Service Account with credentials (JSON key file).
  
### 🚀 Setup & Installation


### 🔩 Configuration

### ▶️ Usage
To run the full ETL process, simply execute the main Python script from your terminal:
```python main.py```
The script will print progress updates to the console, such as the number of new rows added and existing rows updated.
For a more interactive experience or for debugging, you can use the etl_notebook.ipynb in a Jupyter environment.
