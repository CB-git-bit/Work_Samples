import flask
from flask import request
import joblib
import pandas as pd

app = flask.Flask(__name__)

# Load the trained model
model = joblib.load('output/MLmodel.pkl')

# Define the API endpoint
@app.route('/predict', methods=['POST'])
def predict():
    # Parse the request data
    data = request.get_json(force=True)
    df = pd.DataFrame(data)

    # Make predictions using the trained model
    predictions = model.predict(df)

    # Prepare the response
    response = {
        'predictions': predictions.tolist()
    }

    return flask.jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)