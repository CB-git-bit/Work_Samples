# Work_Samples
Work Sample for Data Engineer role.

## Introduction

This repository contains the work sample for the Data Engineer position at RiskThinking.ai. The goal of this project is to assess my technical skills and proficiency in data engineering concepts and tools.

## Requirements

To complete this work sample, you will need the following software and tools:

- Ubuntu 22.04.2 LTS & WSL2
- VS Code 
- Apache Airflow 
- Access to this dataset via a API key: https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset 
- Python 3.7 or higher

*NOTE: This project did not use Docker due to not meeting system requirements. A future iteration of this project may be uploaded in the future upon a system upgrade.

## Project Structure
The project consists of the following files and directories:
- `data_processing.py`: Contains the main functions for performing the project tasks of extracting and transforming the data.
- `MLmodel_v2.py`: Contains code for training the ML model as a function.
- `my_dag.py`: Contains the main code for the data engineering pipeline and dag that is run with Apache Airflow.
- `pipeline_app.py`: Contains code to run the trained ML model on an API service.
- `requirements.txt`: Lists the project dependencies.
- `In_Progress`: Directory for next steps 

## Assumptions & Reasoning in logic
- In problem 2, the rolling median averages is to be applied on "Adjusted Close"
- *Random Forest Regression* was used as suggested in the work sample. This ML algorithm follows a supervised learning approach to split data into groups based on geatures and returns a prediction that is the average across the data it is trained on. This alogrithm is good for continous data like the stock market data being used in this project, which is why I continued to use it. 
- Since a *Random Forest Regression* model was used on stock market data, which is not Gaussian in nature. *Random Forest Regression* is insensitive to features scaling so data standardization was not implemented. Additionally, the data from stocks & ETFs is not Gaussian so outliers would impact any standardizing or normalizing. Therefore, only the data structuring was implemented as part of the pre-processing.

## Instructions
To complete the work sample, follow these instructions:

1. Ensure required software have been installed correctly. See *Resources* for reference on how to.
2. Set up a kaggle API and save your kaggle.json file in the directory home/[user]/.kaggle:
    a. Go to home: 
          ```
          cd home/[user]
          ```
    b. Make your .kaggle directory:
          ```
          mkdir .kaggle
          ```
    c. Save your kaggle.json file here
3. Clone this repository to your local machine.
4. Set up a virtual environment for the airflow project (see Apache Airflow details in *Resources*)
7. Install the project dependencies:
   ```
   pip install -r requirements.txt
   ```
5. Execute the airflow scheduler:
```
   airflow scheduler
   ```
6. Execute the airflow webserver:
```
   airflow webserver
   ```
7. Select stocks *stock_etf_dag* from the selection of dags and run

## Tasks

The work sample consists of the following tasks:

1. Data is downloaded from kaggle using my kaggle API if the data has not already been downloaded
2. The data is extracted from the downloaded zip file using Python's multiprocessing pool.
3. The data is properly formatted and consolidated into one dataframe saved as a parquet.
4. Rolling 30 day average volume is calculated and added as a column for each of the stocks & ETFs.
5. Rolling 30 day median adjusted close is calcuated and added as another column for each of the stocks & ETFs.
6. Data is loaded into a Random Forest Regression model for machine learning with a target of Volume and features specified in steps 4 & 5. 
7. A parameter grid search is implemented on the model to determine the parameters that produce the best score. The model is then retrained with these parameters.
8. Model is to then be deployed to an API service. 

Additional information on the task goals can be found here: https://github.com/RiskThinking/work-samples/blob/main/Data-Engineer.md

## Data

The data for this work sample has been accessed online through a Kaggle API. The dataset can be found under requirements. This data is a zip file containing two subfolders, 'etfs' and 'stocks', which contain CSV files for various etfs and stocks, respectfully. 

## Resources used
* To properly install Ubuntu & WSL 2: https://learn.microsoft.com/en-us/windows/wsl/install
* To operate WSL 2 with VS Code: https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-vscode)
* To install Apache Airflow without Docker: https://www.freecodecamp.org/news/install-apache-airflow-on-windows-without-docker/
* Copilot 
* Websites and other information used is contained in `Additiona_references.txt`



## Contact
If you have any questions or issues with this work sample, please contact the applicant.
