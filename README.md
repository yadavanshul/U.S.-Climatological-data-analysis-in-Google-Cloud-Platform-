# CloudAnalyticsandDataWarehouse-Sem1Project


## Overview
 
The main goal of this research is the analysis of local climate data in the United States from NOAA (National Oceanic and Atmospheric Administration). Understanding American weather trends over time, identifying areas with more noticeable effects of climate change, and helping California's utility companies deal with spikes in demand for electricity are the main objectives.

## File Structure
`CloudAnalyticsHistoricalandRealTimeDataExtraction`: This folder contains data extraction and processing details. It includes scripts and methodologies used to load data from NOAA’s FTP server and API into our system.

`DAG6`: This folder includes the Directed Acyclic Graph (DAG) operations used in Apache Airflow for scheduling and automating the ETL pipeline.

`Weather data - stats and ML`: This folder contains scripts and analysis focusing on our key data science
 use cases.

` dataflowArchived_weather_script` :This file contains the dag configuration.

 `Archive_data_weather` :This is the file fetching configuration from dataflowArchived_weather_script and reading csv file from storage


## Dataset

Access the dataset from NOAA: https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/


## Queries Analysis

The "Queries Analysis" file contains:

- Queries: Set of queries with explanations.
- Screenshots: Executed queries with output.


## Contact Us

For inquiries or feedback, contact:

- Anshul Yadav: anshul.yadav@sjsu.edu

Explore the project and leverage insights from the NOAA weather data!
