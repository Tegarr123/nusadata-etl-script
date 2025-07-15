# nusadata-etl-script

### Brief Overview
The **Nusadata project** aims to comprehensively map the digital divide in Indonesia. The primary goal is to identify, evaluate, and analyze relevant datasets concerning digital access, with the potential to expand into digital usage, literacy, and equity impacts.

### Key Objectives & Activities
The project is structured around four main activities:
- Dataset Identification: Finding and assessing available datasets on digital connectivity in Indonesia.
- Data Analysis: Examining the quality of metadata, interoperability, and identifying gaps in digital divide indicators at both national and sub-national levels.
- Standardization: Developing standardized data models and metadata schemas to enable consistent comparisons across different times, regions, and demographic groups.
- Prototyping: Building a prototype web portal to visualize key indicators, allow data downloads, and support policy-making simulations.

### Team & My Role
- The project team consists of seven members: one project leader, three data clerks, and three dashboard engineers.
- My role as a Dashboard Engineer is crucial for the data infrastructure. I am responsible for designing and implementing the ETL (Extract, Transform, Load) process. This involves taking raw data from our sources, managed by the data clerks, and transforming it into a clean, structured format suitable for the data warehouse.

### Current Data Workflow
The current data pipeline is streamlined and built entirely on the Google ecosystem:
- Data Source: The primary data comes from Badan Pusat Statistik (BPS). This data is managed and maintained by the data clerks within Google Sheets.
- ETL Process: I extract this data from the source sheets, perform necessary transformations (cleaning, standardizing, aggregating), and load it into the data warehouse.
- Data Warehouse: The central data warehouse is also hosted on Google Sheets, which I will interact with programmatically using the Google Sheets API.

![Project Workflow](https://raw.githubusercontent.com/Tegarr123/nusadata-etl-script/refs/heads/main/Workflow.png)
