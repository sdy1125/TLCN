# Agricultural Export Data Lakehouse

This graduation project focuses on the research, design, and implementation of a Data Lakehouse for agricultural export data in Vietnam. The study covers three major commodities: coffee, pepper, and natural rubber.

The system integrates heterogeneous data from FAOSTAT, UN Comtrade, the World Bank, NASA POWER, and provincial agricultural sources. These datasets provide information about production, harvested area, yield, domestic and international prices, export markets, macroeconomic indicators, and daily weather conditions across Vietnam.

## Project objectives

The project aims to build a unified data platform that supports:

- Analysis of national and provincial agricultural production.
- Analysis of prices, export volume, export value, and trading partners.
- Evaluation of the relationship between weather and crop production.
- Comparison of Vietnam with other producing and exporting countries.
- Preparation of reliable data for dashboards and forecasting models.

## System architecture

The platform follows the Bronze, Silver, and Gold architecture. Raw data is preserved in Bronze, cleaned and standardized in Silver, and transformed into analytical fact and dimension tables in Gold.

The environment is deployed with Docker and combines MinIO, Apache Spark, Apache Iceberg, Airflow, dbt, Trino, PostgreSQL, Power BI, and Python. Together, these technologies provide data storage, processing, orchestration, quality control, SQL analytics, visualization, and forecasting capabilities.
