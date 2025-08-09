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
- [Usage](#%EF%B8%8F-usage)

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
![Architecture](images/architecture.png)

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
- Python 3.11
- A Google Cloud Platform (GCP) project.
- Enabled Google Drive API and Google Sheets API in your GCP project.
- A OAuth 2.0 Client ID with credentials (JSON key file).
  
### 🚀 Setup & Installation
1. **Clone the Repository:**
   ```bash
   git clone https://github.com/Tegarr123/nusadata-etl-script.git
   cd nusadata-etl-script
   ```
2. **Set Up a Virtual Environment (Recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```
3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
4. **Google API Credentials (OAuth 2.0):**
   - Go to your [Google Cloud Console](https://console.cloud.google.com/).
   - Create a new project (or use an existing one).
   - Enable the **Google Drive API** and **Google Sheets API**.
   <img src="images/drive.api.png" widht="100"> ![sheets_api](images/sheets.api.png)
   - Go to "APIs & Services" → "Credentials."
   - Click "+ CREATE CREDENTIALS" and select "**OAuth client ID.**"
   - If prompted, configure the "**OAuth consent screen.**" For a personal project, you can choose "**External**" user type and fill in the required app name and email fields.
   ![set_name](images/set.name.auth.platform.png = 100x100) ![consent_audience](images/consent.audience.png = 100x100)
   - For the "Application type," select "**Desktop app.**"
   ![application_type](images/app.type.oauth.png = 100x100)
   - After creation, a window will pop up. Click "**DOWNLOAD JSON**" to get your credentials file.
   ![download_json](images/download.json.credentials.png = 100x100)
   - Rename the downloaded file to ```credentials.json``` and place it in the root directory of this project.
   
### 🔩 Configuration

### ▶️ Usage
To run the full ETL process, simply execute the main Python script from your terminal:
```bash
python main.py
```
The script will print progress updates to the console, such as the number of new rows added and existing rows updated.
For a more interactive experience or for debugging, you can use the ```etl_notebook.ipynb``` in a Jupyter environment.
