import os
import datetime as dt
import logging
import joblib
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import pandas as pd
import pyarrow.parquet as pq
from airflow.models import Variable
from multiprocessing import Pool

def train_model(params):
    '''
    Trains a RandomForestRegressor model using the feature-engineered data and the specified parameters.

    Returns:
        RandomForestRegressor: The trained model.
    '''
    # Open the feature-engineered data pickle file
    print("Pulling data...")
     #Pull the feature eng path from xcom:
    fe_path = Variable.get('feature_eng_path')

     # Open the feature-engineered data pickle file
    df = pd.read_parquet(fe_path)

    # Define the features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'
    X = df[features]
    y = df[target]

    # Split data into train and test sets
    print("Train/Test splitting the data...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create a RandomForestRegressor model with the specified parameters
    model = RandomForestRegressor(**params, random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Get timestamp for logging
    timestamp = dt.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Create directories if they don't exist
    os.makedirs('output', exist_ok=True)

    # Save the model using parquet
    with open('output/MLmodel.pkl', 'wb') as f:
        joblib.dump(model, f)

    # Set up logger
    logging.basicConfig(filename='output/training.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # Create a dictionary to hold the log data
    log_dic = {
        "X test value": X_test,
        "Predicted Value": y_pred,
        "Actual Value": y_test.iloc[0],
        "Mean Abs Err": mae,
        "Mean Squared Err": mse,
        "Timestamp": timestamp
    }

    # Log the data into the logger
    logger.info(log_dic)

    return model


def main():
    # Open the feature-engineered data pickle file
    df = pd.read_parquet('feature_eng.parquet')

    # Define the features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'
    X = df[features]
    y = df[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Get the best parameters for the model
    # Load the best parameters from disk if they exist
    if os.path.exists('best_params.pkl'):
        with open('best_params.pkl', 'rb') as f:
            best_params = joblib.load(f)
            print("Best parameters loaded.")
    else:
        print("Searching for best parameters")
        # Define the parameter grid to search over
        param_grid = {
            'n_estimators': [100, 200, 300],
            'max_depth': [None, 5, 10],
            'min_samples_split': [2, 5],
            'min_samples_leaf': [1, 2]
        }

        # Create a list of tasks
        tasks = [(params,) for params in param_grid]

        # Create a pool of processes
        pool = Pool()

        # Train models in parallel using different parameter sets
        results = pool.map(train_model, tasks)

        # Close the pool
        pool.close()
        pool.join()

        # Get the best model
        best_model = min(results, key=lambda x: x.best_score_)
        best_params = best_model.best_params_
        best_score = best_model.best_score_
        print("Best model scored: ", best_score)

        # Save the best parameters to disk
        with open('best_params.pkl', 'wb') as f:
            joblib.dump(best_params, f)
        
    # Create a RandomForestRegressor model with the best parameters
    train_model(**best_params)

if __name__ == '__main__':
    main()
