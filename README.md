# Hotel Booking Trends and Cancelation Analysis

## Repository Outline
```
Main files:
1. README.md - Desciption and context of project
2. P2M3_galuh_alifani_DAG.py - Apache Airflow's file to run scheduled DAG for data loading, cleaning, and population to Elasticsearch
3. P2M3_galuh_alifani_DAG_graph.jpg - Snapshot of Airflow's scheduled DAG process flow
4. P2M3_galuh_alifani_ddl.txt - Python and SQL syntax to fetch, combine, and create + load dataset to PostgreSQL database table
5. P2M3_galuh_alifani_GX.ipynb - Notebook that contains great expectation validations to validate several parameters of the cleaned dataset
6. P1M2_galuh_alifani_inf.ipynb - Notebook that contains data inference exercise using our predictive model from notebook #2 above
7. images folder - Snapshots of Kibana Report containing Dashboard & Plots of the EDA

Other files:
- P2M3_galuh_alifani_conceptual.txt - Conceptual questions regarding NoSQL & Batch Processing
```

## Problem Background
The hospitality industry has experienced major shifts in recent years, particularly due to the COVID-19 pandemic, changes in traveler behaviors, and a surge in online bookings. According to Deloitte’s 2023 Travel Industry Outlook, recovery in the hotel sector has been uneven, with rising customer expectations and high cancellation rates becoming key challenges. 

## Project Output
The output of this project is a Kibana dashboard snapshot that is presented as a report, highlighting key insights of the EDA and business strategy recommendations

## Data
Link to dataset raw file: https://ars.els-cdn.com/content/image/1-s2.0-S2352340918315191-mmc2.zip

The dataset was collected by Nuno Antonio, Ana de Almeida, and Luis Nunes as presented in their paper in [sciencedirect](https://www.sciencedirect.com/science/article/pii/S2352340918315191). 

Description of the dataset as per stated in their paper: 
> "The data describes two datasets with hotel demand data from two hotels. One of the hotels (H1) is a resort hotel and the other is a city hotel (H2). Each observation represents a hotel booking. Both datasets comprehend bookings due to arrive between the 1st of July of 2015 and the 31st of August 2017"

The dataset contains 119k hotel booking records from two hotels in Portugal spanning from June 2015 – August 2017. The dataset includes details of booking records of two hotel types, details of guests, sales channel, average daily rate, its cancelation status, and more behavioral indicators.

## Method
The method used in this project is Exploratory Data Analysis using ElasticSearch and Kibana, which utilized Apache Airflow's scheduled DAG for the data cleaning and migration process to ES.

## Stacks
Tech stacks include:
- Docker
- Apache Airflow
- Elasticsearch
- Kibana
- PostgreSQL
- Python
- Pandas & numpy

## Reference
- [Paper Reference of Data Source](https://www.sciencedirect.com/science/article/pii/S2352340918315191)
- [Data Source](https://ars.els-cdn.com/content/image/1-s2.0-S2352340918315191-mmc2.zip)

---

## Contact
Galuh Adika Alifani
galuh.adika@gmail.com

## Others
### Kibana Plots Snapshots
![Plot 1](<images/plot & insight 01.png>)
![Plot 2](<images/plot & insight 02.png>)
![Plot 3](<images/plot & insight 03.png>)
![Plot 4](<images/plot & insight 04.png>)
![Plot 5](<images/plot & insight 05.png>)
![Plot 6](<images/plot & insight 06.png>)
![Plot 7](<images/plot & insight 07.png>)
![Plot 8](<images/plot & insight 08.png>)
![Plot 9](<images/plot & insight 09.png>)
![Plot 10](<images/plot & insight 10.png>)
![Plot 11](<images/plot & insight 11.png>)

---