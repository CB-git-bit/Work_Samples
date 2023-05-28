# import necessary libraries
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# import project specific modules
from data_processing import *
from MLmodel import *



# create DAG instance
dag = DAG(
    dag_id="stock_etf_dag",
    default_args={"retries": 1},
    description="A simple DAG to run through the ML pipeline of predicting stock and ETF volumes from volume moving average and the rolling median of the adj_close.",
    start_date=datetime(2023, 5, 2),
    schedule_interval=timedelta(hours=1),
    catchup=False
)

# Problem 1: Raw Data Processing

# get data
get_data = PythonOperator(
    task_id="Download_Data",
    python_callable=download_data,
    dag=dag
)

# extract csv
extract_data = PythonOperator(
    task_id="Extract_Data",
    python_callable=extract_csvs,
    dag=dag
)
# add delay of 1 second between extract_data and format_data
delay = PythonOperator(
    task_id="Delay",
    python_callable=lambda: time.sleep(1),
    dag=dag
)
# structure data
format_data = PythonOperator(
    task_id="Structure_Data",
    python_callable=structure_data,
    dag=dag
)
# add delay of 1 second between format_data and feature_eng1
delay2 = PythonOperator(
    task_id="Delay2",
    python_callable=lambda: time.sleep(1),
    dag=dag
)

# Problem 2: Feature Engineering
feature_eng1 = PythonOperator(
    task_id="Rolling_Volume_Average",
    python_callable=calculate_vol_moving_avg,
    dag=dag
)

feature_eng2 = PythonOperator(
    task_id="Rolling_AdjClose_Median",
    python_callable=calculate_adj_close_rolling_med,
    dag=dag
)

# Problem #3: Train ML model
# training = PythonOperator(
#     task_id="Train_ML_Model",
#     python_callable=train_model,
#     dag=dag
# )

training = BashOperator(
    task_id="Train_ML_Model",
    bash_command='python MLmodel_v2.py',
    dag=dag
)


# Problem #4: Serve model
serve_model = BashOperator(
    task_id="Serve_Model",
    bash_command='python pipeline_app.py',
    dag=dag
)

# show dependencies
get_data >> extract_data >> delay >> format_data >> delay2 >> feature_eng1 >> feature_eng2 >> training >> serve_model
# Set dependencies to avoid concurrent access to the SQLite database
# extract_data.set_downstream(format_data)
# feature_eng1.set_upstream(format_data)