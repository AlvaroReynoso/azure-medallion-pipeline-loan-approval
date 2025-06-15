# 🔷 Pipeline Azure Modelo Medallón – Datos de Aprobación de Préstamos (Español)

Este proyecto implementa un pipeline ETL completo de punta a punta utilizando la **arquitectura Medallón (Bronce, Plata, Oro)** en Azure, procesando un dataset de solicitudes de préstamos bancarios.

 **Objetivo**: Limpiar y preparar datos crudos de aplicaciones de préstamos para generar insights como tasas de aprobación, análisis de ingresos y segmentación de solicitantes.

🔧 **Tecnologías Utilizadas**:
- **Azure Data Factory**: Orquestación del pipeline
- **Azure Databricks**: Transformación de datos con PySpark
- **Delta Lake**: Formato de almacenamiento escalable
- **Azure Data Lake Storage Gen2**: Almacenamiento de datos
- **Power BI** *(opcional)*: Visualización de insights de negocio

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

Descripcion: En este pipeline medallon se busca ingerir datos desde un archivo .csv almacenado en Data Lake, luego en la capa "Bronce" se busca transformar parcialmente las estructuras de columnas crudas (raw estructured)  para crear un modelo de delta table crudo (raw) y almacenar .parquet y .csv, en capa plata se busco limpiar datos insconsistentes y transformar tipos de datos para un manejo correcto.
Finalmente en la capa "oro" se busca crear modelos de agrupacion, porcentajes y promedios para posteriormente preparar KPIs.
Todo se manejo con un sistema de almacenamiento ordenado y creando tres directorios por capa. 



# Azure Medallion Pipeline – Loan Approval Data (English)

This project implements a complete end-to-end ETL pipeline using the **Medallion architecture (Bronze, Silver, Gold)** on Azure, processing a bank loan approval dataset.

**Goal**: Clean and prepare raw loan application data to generate insights such as approval rates, income analysis, and applicant segmentation.

🔧 **Tools Used**:
- Azure Data Factory: Orchestration of pipeline
- Azure Databricks: Data transformation with PySpark
- Delta Lake: Scalable storage format
- Azure Data Lake Storage Gen2: Data storage
- Power BI (optional): Visualize business insights

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

**Description**: This medallion pipeline ingests data from a .csv file stored in a Data Lake. The Bronze layer partially transforms the raw column structures to create a raw delta table model and store .parquet and .csv files. The Silver layer cleans inconsistent data and transforms data types for proper management.
Finally, the Gold layer creates clustering models, percentages, and averages to later prepare KPIs.
Everything is managed with an organized storage system, creating three directories per layer.

🔐 Requisitos previo: 
Se creo un secret Scope en databricks secrets para alojar una llave o key.
Con este mecanismo, Databricks puede conectarse a tu contenedor sin necesidad de poner la clave en el código y exponerla.


Imagenes / Images
![PIPE](https://github.com/user-attachments/assets/daa9852b-fd1d-4f54-af7f-68c0cbfbe5ea)

![check databricks](https://github.com/user-attachments/assets/07730931-b891-4eb8-a63a-626fd57ae91e)

![capas 3](https://github.com/user-attachments/assets/44bb79da-7907-4b81-8747-96baf5dab7c0)



