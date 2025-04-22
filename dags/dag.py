'''
=================================================
This program is created to automate the process of extracting, cleaning, and populating data from PostgreSQL to ElasticSearch in order to be processed further in Kibana for visualization.
The dataset used contains 119k hotel booking records from two hotels in Portugal from the year 2015-2017.
=================================================
'''

from airflow.models import DAG

from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch, helpers

from datetime import datetime
import pandas as pd
import psycopg2 as db
import numpy as np

def sql_read(table) : 
    return f"""
    SELECT * FROM {table}
    """

def load_data(dbDetails, tableName):
    '''
    This function is used to retrieve raw data from a PostgreSQL database and save it as a JSON file for further cleaning.

    Parameters:
    - dbDetails: a dictionary containing database connection details (dbname, hostname, username, password, port)
    - tableName: the name of the table to be queried from the PostgreSQL database

    Process:
    - Establishes a connection to the PostgreSQL database
    - Executes a SQL query to fetch data
    - Saves the data in JSON format to a local file

    Returns:
    None: the function saves a file named 'table_m3.json' to disk and print a success message.

    Example usage:

    dbDetails = {
        "dbname": 'airflow',
        "hostname": 'localhost', 
        "username": 'airflow',
        "password": 'airflow',
        "port": '5434'
    }
        
    load_data(dbDetails, "table_m3")
    '''

    dbname, hostname, username, password, port = dbDetails['dbname'], dbDetails['hostname'], dbDetails['username'], dbDetails['password'], dbDetails['port']
    conn_string=f"dbname={dbname} host={hostname} user={username} password={password} port={port}"
    conn=db.connect(conn_string)
    df_psql=pd.read_sql(sql_read(tableName),conn)
    df_psql.to_json(f'/opt/airflow/dags/{tableName}.json', orient='records')
    conn.close()
    print("data has been loaded")

def clean_data(jsonFile, cleanFileName):
    '''
    This function performs data cleaning and preprocessing on the raw data 
    previously saved in JSON format. The final result is a clean CSV file 
    ready for analysis or machine learning.

    Parameters:
    - jsonFile: the name of the JSON file containing raw data to be cleaned
    - cleanFileName: the name of the output CSV file to save the cleaned data

    Cleaning steps include:
    - Replacing coded hotel type values with readable labels
    - Standardizing column names to snake_case and lowercase
    - Trimming whitespace from string columns
    - Dropping duplicate rows
    - Handling missing values
    - Fixing column data types
    - Creating new derived columns (e.g. arrival_date, total_stay_nights)
    - Removing logically invalid rows

    Returns:
    None: the function saves a cleaned CSV file named 'data_clean.csv' and print a success message

    Example usage:
    clean_data("table_m3.json", "data_clean.csv")
    '''
    df_raw = pd.read_json(f'/opt/airflow/dags/{jsonFile}')
    df = df_raw.copy()

    # 1. replace hotelType values to meaningful names
    df['HotelType'] = df['HotelType'].replace({'H1': 'resort_hotel', 'H2': 'city_hotel'})

    # 2. Due to the unique nature of each rows, but the data is without unique identifier, we need to create a synthetic 'RecordNumber' per arrival date to identify each booking order per date (since there is no unique room number identifier in the data)
    df['RecordNumber'] = df.groupby(['HotelType', 'ArrivalDateYear', 'ArrivalDateMonth', 'ArrivalDateDayOfMonth']).cumcount() + 1

    # 3. standardize column names: lowercase + snake_case + remove symbols/numbers/trailing spaces
    df.columns = (
        df.columns
        .str.replace(r'(?<=[a-z])(?=[A-Z])', '_', regex=True) # split camelCase
        .str.replace(r'[\s-]+', '_', regex=True) # replace spaces or dashes with underscore
        .str.replace(r'[^a-zA-Z_\s]', '', regex=True) # remove symbols and numbers except underscore
        .str.lower()
        .str.strip() # remove trailing spaces
    )

    # 4. clean column values: remove trailing spaces
    for col in df.select_dtypes(include='object'):
        df[col] = df[col].str.strip()
    
    # 5. drop duplicates
    df = df.drop_duplicates()

    # 6. handle missing values
    non_std_mv = ['na', 'n/a', 'nan', 'null', 'undefined', 'unknown']
    replacement_map = {
        'children': 0,
        'meal': 'SC', # SC = no meal package in the paper definition
        'country': 'Unknown',
        'market_segment': 'Unknown',
        'distribution_channel': 'Unknown',
        'agent': 'No Agent',
        'company': 'No Company'
    }
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(
                lambda x: np.nan if str(x).strip().lower() in non_std_mv else x
            )
    for col, replacement in replacement_map.items():
        df[col] = df[col].fillna(replacement)

    # 7. clean column types:
    # 'children' from object to int
    df['children'] = df['children'].astype('int64')

    # 8. Create new columns:
    # arrival_date: combine arrival year, month, day
    df['arrival_date_month'] = pd.to_datetime(df['arrival_date_month'], format='%B').dt.month
    df['arrival_date'] = pd.to_datetime(dict(
        year=df['arrival_date_year'],
        month=df['arrival_date_month'],
        day=df['arrival_date_day_of_month']
    ))
    # is_family: if there are children or babies, then family = Yes, else No
    df['is_family'] = df[['children', 'babies']].gt(0).any(axis=1).map({True: 1, False: 0})
    # total_stay_nights = total stay weekend + total stay weekdays
    df['total_stay_nights'] = df['stays_in_weekend_nights'] + df['stays_in_week_nights']
    # total_rate: ADR * length of stay
    df['total_rate'] = df['adr'] * df['total_stay_nights']
    # is_room_type_changed: if reserved room type is different from assigned room type
    df['is_room_type_changed'] = df['reserved_room_type'].ne(df['assigned_room_type']).map({True: 1, False: 0})

    # 9. remove rows with logical errors:
    # rows where total guests == 0 AND booking is not cancelled
    df['total_guests'] = df['adults'] + df['children'] + df['babies']
    df = df[~((df['total_guests'] == 0) & (df['is_canceled'] == 0))]
    # stay_for_free with 0 nights: records with 0 total stay nights, 0 ADR, AND status is check-out (instead of canceled or no-show)
    df = df[~((df['total_stay_nights'] == 0) & (df['adr'] == 0) & (df['reservation_status'] == 'Check-Out'))]
    # rows where guests are only children or babies with no adults
    df = df[~((df['adults'] == 0) & ((df['children'] > 0)) | ((df['babies'] > 0)))]
    df.to_csv(f'/opt/airflow/dags/{cleanFileName}', index=False)

    print("data has been cleaned")

def to_elasticsearch(esLink, indexName, csvFile):
    '''
    This function uploads the cleaned CSV data to an Elasticsearch index 
    for easy querying and dashboarding.

    Process:
    - Loads the cleaned data from CSV
    - Converts each row into a JSON document
    - Uses bulk upload to send the data to Elasticsearch

    Returns:
    None: but prints the number of documents successfully indexed.

    Example usage:
    to_elasticsearch("http://elasticsearch:9200", "table_m3", "data_clean.csv")
    '''
    es = Elasticsearch(esLink)
    index_name = indexName

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    
    df = pd.read_csv(f'/opt/airflow/dags/{csvFile}')

    actions = [
        {
            "_index": index_name,
            "_source": row.to_dict()
        }
        for _, row in df.iterrows()
    ]

    success, _ = helpers.bulk(es, actions)
    print(f"{success} documents indexed to Elasticsearch '{index_name}'.")

default_args= {
    'owner': 'Galuh',
    'start_date': datetime(2024, 11, 1)
}

with DAG(
    "M3_GA_pipeline",
    description='GA_M3 end-to-end pipeline',
    schedule_interval='10,20,30 9 * * 6',
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1
    fetchData = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=load_data,
        op_args=[{
            "dbname": 'airflow',
            "hostname": 'postgres', 
            "username": 'airflow',
            "password": 'airflow',
            "port": '5432'
        }, "table_m3"]
    )

    # task: 2
    cleanData = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean_data,
        op_args=["table_m3.json", "data_clean.csv"]
    )
    
    # task: 3
    toElasticsearch = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=to_elasticsearch,
        op_args=["http://elasticsearch:9200", "table_m3", "data_clean.csv"]

    )

    fetchData >> cleanData >> toElasticsearch