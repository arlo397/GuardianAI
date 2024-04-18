#!/usr/bin/env python3
from flask import Flask, jsonify, abort, request, Response
import pandas as pd
import logging
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

@app.route('/data_example', methods=['GET'])
def get_data() -> Response:
    """
    Fetches and returns the first five records of the dataset as JSON.

    Returns:
        flask.Response: A JSON response containing the first five records of the dataset.

    Raises:
        HTTPException: An error 500 if the data cannot be loaded due to an internal error.
    """
    data_path = './data/fraud_test.csv'
    try:
        df = pd.read_csv(data_path)
        logging.info("Data loaded successfully.")
        return jsonify(df.head().to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        abort(500)



@app.route('/amt_analysis', methods=['GET'])
def amt_analysis() -> Response:
    """
    Computes and returns statistical descriptions of the transaction amounts in the dataset.

    Returns:
        flask.Response: A JSON response containing statistical summaries of the 'amt' field in the dataset,
                         including count, mean, std, min, 25%, 50%, 75%, and max.

    Raises:
        HTTPException: An error 500 if the data cannot be loaded or statistics cannot be computed due to an internal error.
    """
    data_path = './data/fraud_test.csv'
    try:
        df = pd.read_csv(data_path)
        amt_stats = df['amt'].describe()
        stats_dict = amt_stats.to_dict()

        return jsonify(stats_dict)
    except Exception as e:
        logging.error(f"Error loading data or computing statistics: {e}")
        abort(500)


@app.route('/amt_fraud_correlation', methods=['GET'])
def compute_correlation() -> Response:
    """
    Computes the correlation between transaction amount ('amt') and fraud status ('is_fraud') in the dataset.

    Returns:
        flask.Response: A JSON response containing the correlation matrix between 'amt' and 'is_fraud'.

    Raises:
        HTTPException: An error 500 if the data cannot be loaded or the correlation cannot be computed due to an internal error.
    """
    data_path = './data/fraud_test.csv'
    try:
        df = pd.read_csv(data_path)
        correlation = df[['amt', 'is_fraud']].corr()
        logging.info("Correlation computed successfully.")
        return jsonify(correlation.to_dict())
    except Exception as e:
        logging.error(f"Error computing correlation: {e}")
        abort(500)


@app.route('/fraudulent_zipcode_info', methods=['GET'])
def fraudulent_zipcode_info() -> Response:
    """
    Identifies the zipcode with the highest number of fraudulent transactions, and retrieves its geographic location.

    Returns:
        flask.Response: A JSON response containing the most fraudulent zipcode, the number of frauds, and a Google Maps link to the location.

    Raises:
        HTTPException: An error 500 if there is a failure in data handling or API interaction, or a 404 if no location can be found for the zipcode.
    """
    data_path = './data/fraud_test.csv'
    bing_api_key = "AoaZqu_awoToijquulNRBaNbW98dniWa17O-QGrlBxP6Nv60C-3YaMIDkLqNb5UL"

    try:
        df = pd.read_csv(data_path)
        fraud_transactions = df[df['is_fraud'] == 1]
        fraudulent_zipcode_counts = fraud_transactions['zip'].astype(str).value_counts()

        most_fraudulent_zipcode = fraudulent_zipcode_counts.idxmax()
        max_fraud_count = fraudulent_zipcode_counts.max()

        response = requests.get(
            f"http://dev.virtualearth.net/REST/v1/Locations/US/{most_fraudulent_zipcode}",
            params={"key": bing_api_key}
        )
        data = response.json()
        if response.status_code == 200 and data['resourceSets'][0]['resources']:
            location_data = data['resourceSets'][0]['resources'][0]
            lat, lon = location_data['point']['coordinates']
            google_maps_link = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"

            result = {
                "most_fraudulent_zipcode": most_fraudulent_zipcode,
                "fraud_count": max_fraud_count,
                "latitude": lat,
                "longitude": lon,
                "Google Maps Link": google_maps_link
            }
            return jsonify(result)
        else:
            return jsonify({"error": "Location not found for the most fraudulent zipcode"}), 404

    except Exception as e:
        logging.error(f"Error loading data or computing statistics or fetching location: {e}")
        abort(500)



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')


