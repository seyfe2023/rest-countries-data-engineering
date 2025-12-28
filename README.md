# **REST Countries Data Engineering Pipeline** #

End-to-end ETL pipeline using **Apache Airflow, Python, PostgreSQL, and Docker**

This project demonstrates a production-style data engineering workflow that ingests country data from a public API, processes it, and loads it into a relational data warehouse.
The pipeline is fully orchestrated with Apache Airflow, containerized with Docker, and designed to reflect real-world data engineering practices.


# **Key Features**


**End-to-End ETL:** Automated extraction, transformation, and loading.

**Orchestration:** Apache Airflow with retries and task logging.

**Data Modeling:** Staging (JSONB) to Dimensional modeling in PostgreSQL.

**Containerization:** Fully Dockerized environment for easy deployment.



# **Architecture Overview**

┌──────────────────────┐
│  REST Countries API  │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│  Extract (Python)    │
│  - API requests      │
│  - Error handling    │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│  Transform (Python) │
│  - Cleaning          │
│  - Normalization     │
│  - Schema mapping    │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│ PostgreSQL           │
│  - Staging (JSONB)   │
│  - Dim tables        │
└─────────┬────────────┘
          │
          ▼
┌──────────────────────┐
│ Apache Airflow       │
│  - Scheduling        │
│  - Retries           │
│  - Monitoring        │
└──────────────────────┘



# **Airflow DAG Design**

DAG: rest_countries_etl
Schedule: Daily (@daily)

create_tables
      │
      ▼
   extract
      │
      ▼
  transform
      │
      ▼
     load


# **Project Structure**

rest-countries-de/
│
├── dags/
│   └── rest_countries_etl_dag.py     # Airflow DAG definition
│
├── src/
│   ├── extract.py                    # API ingestion
│   ├── transform.py                 # Data cleaning & normalization
│   └── load.py                      # Database loading
│
├── sql/
│   └── create_tables.sql             # DDL (staging & dimensions)
│
├── airflow/                          # Airflow runtime config
├── logs/                             # Airflow logs (mounted volume)
├── plugins/                          # Optional Airflow plugins
│
├── docker-compose.yml                # Service orchestration
├── requirements.txt                  # Python dependencies
├── .env                              # Environment variables
└── README.md



# **Data Model**

Staging Table – stg_countries_raw

| Column      | Type      | Description            |
| ----------- | --------- | ---------------------- |
| source      | TEXT      | Data source identifier |
| payload     | JSONB     | Raw API response       |
| ingested_at | TIMESTAMP | Load timestamp         |

Dimension Table – dim_country

| Column       | Description        |
| ------------ | ------------------ |
| country_code | ISO country code   |
| country_name | Country name       |
| region       | World region       |
| subregion    | Sub-region         |
| population   | Population count   |
| area         | Country area (km²) |


How to Run Locally
1. Start Services:
   docker compose up -d
2. Access Airflow:
   Go to http://localhost:8080. Default credentials: admin / admin.
3. Verify Data:
   SELECT COUNT(*) FROM dim_country;


   
