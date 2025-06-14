# 🔷 Pipeline Azure Modelo Medallón – Datos de Aprobación de Préstamos (Español)

Este proyecto implementa un pipeline ETL completo de punta a punta utilizando la **arquitectura Medallón (Bronce, Plata, Oro)** en Azure, procesando un dataset de solicitudes de préstamos bancarios.

**Objetivo**: Limpiar y preparar datos crudos de aplicaciones de préstamos para generar insights como tasas de aprobación, análisis de ingresos y segmentación de solicitantes.

🔧 **Tecnologías Utilizadas**:
- **Azure Data Factory**: Orquestación del pipeline
- **Azure Databricks**: Transformación de datos con PySpark
- **Delta Lake**: Formato de almacenamiento escalable
- **Azure Data Lake Storage Gen2**: Almacenamiento de datos

📁 **Estructura del Pipeline**:
1. **Capa Bronce**: Ingesta de datos crudos desde archivo CSV.
2. **Capa Plata**: Limpieza, formateo y tipado de datos.
3. **Capa Oro**: Agregaciones y métricas clave listas para análisis.

📌 **Notebooks Incluidos**:
- `ETL_databank_loans_config`: Configuración de montado y rutas
- `ETL_databank_loans_bronce`: Lectura del CSV crudo y guardado en Delta y CSV
- `ETL_databank_loans_plata`: Limpieza y estandarización del dataset
- `ETL_databank_loans_oro`: Generación de KPIs como tasas de aprobación

🧠 Habilidades:
- Ingeniería de Datos en Azure
- Delta Lake y arquitectura Medallón
- PySpark y orquestación de pipelines



<hr>




# 🔷 Azure Medallion Pipeline – Loan Approval Data (English)

This project implements a complete end-to-end ETL pipeline using the **Medallion architecture (Bronze, Silver, Gold)** on Azure, processing a bank loan approval dataset.

**Goal**: Clean and prepare raw loan application data to generate insights such as approval rates, income analysis, and applicant segmentation.

🔧 **Tools Used**:
- Azure Data Factory: Orchestration of pipeline
- Azure Databricks: Data transformation with PySpark
- Delta Lake: Scalable storage format
- Azure Data Lake Storage Gen2: Data storage

📁 **Pipeline Structure**:
1. **Bronze Layer**: Raw data ingestion from CSV.
2. **Silver Layer**: Data cleaning, formatting, and type casting.
3. **Gold Layer**: Aggregations and KPIs ready for analysis.

📌 Notebooks Included:
- `ETL_databank_loans_config`: Mounts and paths configuration
- `ETL_databank_loans_bronce`: Reads raw CSV and stores Delta & CSV
- `ETL_databank_loans_plata`: Cleans and standardizes the dataset
- `ETL_databank_loans_oro`: Generates approval rate KPIs

🧠 Skills:
- Data Engineering with Azure
- Delta Lake and Medallion Architecture
- PySpark and pipeline orchestration
