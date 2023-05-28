import zipfile
import pandas as pd
import os
import kaggle
import pyarrow.parquet as pq
from multiprocessing import Pool

from airflow.models import Variable
from airflow.operators.python import task


'''
# Problem 1: Raw Data Processing
**Objective**: Ingest & process raw stock market data
 
     - ensure data structures are as requested
     - output shoudl be a structured fromat

**Data Source**: https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset
**Project Requirements**: https://github.com/RiskThinking/work-samples/blob/main/Data-Engineer.md
'''
#ensures dataset exitss
def download_data():  
    zip_path = 'stock-market-dataset.zip'

    # Check if data already exists
    if not os.path.exists(zip_path):
        print("Path does not exist, downloading data...")
        # Log on to Kaggle
        kaggle.api.authenticate()
        # Download the dataset
        kaggle.api.dataset_download_files(dataset='jacksoncrow/stock-market-dataset', path='./', force=True)
        print("Download complete.")

    return zip_path


#extracts the files from the zip fold
def extract_csvs():
    zip_path = 'stock-market-dataset.zip'
    csv_path = 'AllCSVs'
    exclude_file = 'etfs/PRN.csv' #specify this file because the PRN stock does not exist and continuously through up errors
    # future improvement: have this error be a try & except so can prevent from happening in future
    if not os.path.exists(csv_path): #check if data has already been extracted
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            
            # Exclude file and extract the rest
            print("Extracting files...")

            # Define a helper function for parallel extraction
            def extract_file(file):
                if file != exclude_file:
                    zip_ref.extract(file, csv_path)

            # Extract files in parallel via multiprocessing pool     
            with Pool() as pool:
                pool.map(extract_file, file_list)

            print("Extraction complete.")
    
    return csv_path
            
#Process the data in chunks for efficiency 
def process_chunk(chunk, filename):
    # Process the chunk as needed
    # Ensure columns have desired data types
    chunk['Date'] = pd.to_datetime(chunk['Date'], format='%Y-%m-%d').dt.strftime('%Y-%m-%d')
    chunk['Open'] = chunk['Open'].astype(float)
    chunk['High'] = chunk['High'].astype(float)
    chunk['Low'] = chunk['Low'].astype(float)
    chunk['Close'] = chunk['Close'].astype(float)
    chunk['Adj Close'] = chunk['Adj Close'].astype(float)
    chunk['Volume'] = pd.to_numeric(chunk['Volume'], errors='coerce')

    # Extract the symbol from the filename and add as a new column
    symbol = filename.split('.')[0]
    chunk['Symbol'] = symbol

    return chunk

# Process the chunk and filename
def process_chunk_file(chunk_file):
    chunk, filename = chunk_file
    return process_chunk(chunk, filename)

#structure the data into the appropriate methods
#use previosu function
def structure_data():
    subdirs = ['etfs', 'stocks']
    dir_path = "./AllCSVs"
    dfs = []

    for subdir in subdirs:
        subdir_path = os.path.join(dir_path, subdir)
        
        for filename in os.listdir(subdir_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(subdir_path, filename)
                # Read the CSV file into a dataframe in chunks
                for chunk in pd.read_csv(file_path, chunksize=10000):
                    # Create a tuple of the chunks w filenames
                    dfs.append((chunk, filename))

    # Process the chunks in parallel using multiprocessing pool
    with Pool() as pool:
        #process the dfs in chunks in the pools
        processed_chunks = pool.map(process_chunk_file, dfs) 
        #zip(*dfs) converts the chunk & filename as separte arguments for the process_chunk_file function

    
    # Concatenate all the dataframes into one
    df_all = pd.concat(processed_chunks, ignore_index=True)
    df_all.dropna(inplace=True)
    df_all.set_index('Date', inplace=True)

    #save a parquet for easy extraction and use in future tasks
    structure_data_path = 'structured_data.parquet'
    df_all.to_parquet(structure_data_path)

    # Push the structured data path to XCom to be used in other tasks
    Variable.set("structured_data", structure_data_path)

    return df_all

#Process to run all functions & return the structured data
def get_processed_data():
    download_data()
    extract_csvs()
    df_all = structure_data()

'''
# Problem 2: Feature Engineering
**Objective**: Build some feature engineering on top of the dataset from Problem 1
 
    - Calculate the moving average of the trading volume (Volume) of 30 days per each stock and ETF, and retain it in a newly added column vol_moving_avg.
    - Similarly, calculate the rolling median and retain it in a newly added column adj_close_rolling_med.
    - Retain the resulting dataset into the same format as Problem 1, but in its own stage/directory distinct from the first.
    - (Bonus) Write unit tests for any relevant logic.
'''

# feature_engineering.py module

def calculate_vol_moving_avg():

    # Pull the structured data from XCom
    xcom_data = Variable.get("structured_data")

    #Open the structured data frame 
    df = pd.read_parquet(xcom_data)


    print("Calculating Moving Average on Volume...")
    '''Calculate the moving average of the trading volume (Volume) 
    of 30 days per each stock and ETF, and retain it in a newly added
    column vol_moving_avg.'''
    df['vol_moving_avg'] = df.groupby('Symbol')['Volume'].transform(lambda x: x.rolling(window=30).mean())
    df['vol_moving_avg'].fillna(0, inplace=True)
    print("Moving avg is calculated. Saving to pickle file...")
    
    #save updated data as a new parquet file
    vol_move_path = 'vol_moving_avg.parquet'
    df.to_parquet(vol_move_path)

    #Push the vol_moving_avg path to xcom
    Variable.set("vol_moving_avg", vol_move_path)

    return vol_move_path


def calculate_adj_close_rolling_med(): 
    # Pull the structured data from XCom
    xcom_data = Variable.get("vol_moving_avg")
    df = pd.read_parquet(xcom_data)
    
    print("Calculating Rolling Median on Adj Close...")
    df['adj_close_rolling_med'] = df.groupby('Symbol')['Adj Close'].transform(lambda x: x.rolling(window=30).median())
    df['adj_close_rolling_med'].fillna(0, inplace=True)
    print("Rolling median is calculated. Saving to pickle file...")

    #save updated data as a new parquet file
    feature_engineering_path = 'feature_eng.parquet'
    df.to_parquet(feature_engineering_path)

    #Push the feature_engineering path to xcom
    Variable.set("feature_eng", feature_engineering_path)

    return feature_engineering_path




