#!/usr/bin/env python3
from flask import Flask, jsonify, abort, request, Response
from jobs import logging, logging_level, format_str, rd, add_job, get_job_by_id, get_all_job_ids, get_job_result, delete_all_jobs

import json
import pandas as pd
import logging
import os
import requests

#TODO: Later this will need to change since we shouldn't be accessing the raw data this way
current_directory = os.path.dirname(__file__)

# Modify the DATA_PATH to point to the directory above src
DATA_PATH = os.path.join(current_directory, '..', 'data', 'fraud_test.csv')

app = Flask(__name__)
logging.basicConfig(filename='logger.log', format=format_str, level=logging.DEBUG, filemode='w')

# curl localhost:5000/data -X GET
@app.route('/data', methods=['GET', 'POST', 'DELETE'])
def data():
    # POST - Put data into Redis
    if request.method == 'POST':
        df = pd.read_csv(DATA_PATH)
        data:list = df.head().to_dict(orient='records')
        # Iterate over data and store in redis database
        id_num = 0
        for fraud_dict in data:
            # Input data into database as a string
            rd.set(id_num, json.dumps(fraud_dict))   
            id_num += 1         

        logging.info("Data POSTED into Redis Database \n")
        return ("Data POSTED into Redis Database \n")

    # GET - Return all data from Redis
    # Read all data out of Redis and return it as a JSON list.
    elif request.method == 'GET':
        # Iterate over the keys in redis and return everthing as JSON
        data = []
        for trans_id in rd.keys():
            data.append(json.loads(rd.get(trans_id)))

        if len(data) == 0:
            logging.info("No data stored in Redis Database. \n")
            return("No data stored in Redis Database. \n")
        
        logging.info("Data RETRIVED from Redis Database. \n")
        return data

    # DELETE - Delete data in Redis
    elif request.method == 'DELETE':
        for trans_id in rd.keys():
            rd.delete(trans_id)
        logging.info("Data DELETED from Redis Database \n")
        return ("Data DELETED from Redis Database \n")
    else:
         pass

# curl localhost:5000/data_example -X GET
@app.route('/data_example', methods=['GET'])
def get_data_example() -> Response:
    """
    Fetches and returns the first five records of the dataset as JSON.

    Returns:
        flask.Response: A JSON response containing the first five records of the dataset.

    Raises:
        HTTPException: An error 500 if the data cannot be loaded due to an internal error.
    """
    try:
        df = pd.read_csv(DATA_PATH)
        logging.info("Data loaded successfully.")
        return jsonify(df.head().to_dict(orient='records'))
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        abort(500)

# curl localhost:5000/amt_analysis -X GET
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
    try:
        df = pd.read_csv(DATA_PATH)
        amt_stats = df['amt'].describe()
        stats_dict = amt_stats.to_dict()

        return jsonify(stats_dict)
    except Exception as e:
        logging.error(f"Error loading data or computing statistics: {e}")
        abort(500)

# curl localhost:5000/amt_fraud_correlation -X GET
@app.route('/amt_fraud_correlation', methods=['GET'])
def compute_correlation() -> Response:
    """
    Computes the correlation between transaction amount ('amt') and fraud status ('is_fraud') in the dataset.

    Returns:
        flask.Response: A JSON response containing the correlation matrix between 'amt' and 'is_fraud'.

    Raises:
        HTTPException: An error 500 if the data cannot be loaded or the correlation cannot be computed due to an internal error.
    """
    try:
        df = pd.read_csv(DATA_PATH)
        correlation = df[['amt', 'is_fraud']].corr()
        logging.info("Correlation computed successfully.")
        return jsonify(correlation.to_dict())
    except Exception as e:
        logging.error(f"Error computing correlation: {e}")
        abort(500)

# curl localhost:5000/fraudulent_zipcode_info -X GET
@app.route('/fraudulent_zipcode_info', methods=['GET'])
def fraudulent_zipcode_info() -> Response:
    """
    Identifies the zipcode with the highest number of fraudulent transactions, and retrieves its geographic location.

    Returns:
        flask.Response: A JSON response containing the most fraudulent zipcode, the number of frauds, and a Google Maps link to the location.

    Raises:
        HTTPException: An error 500 if there is a failure in data handling or API interaction, or a 404 if no location can be found for the zipcode.
    """
    bing_api_key = "AoaZqu_awoToijquulNRBaNbW98dniWa17O-QGrlBxP6Nv60C-3YaMIDkLqNb5UL"

    try:
        df = pd.read_csv(DATA_PATH)
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

# curl localhost:5000/jobs -X GET
# curl localhost:5000/jobs -X DELETE
# curl localhost:5000/jobs -X POST -d '{}' -H "Content-Type: application/json"
@app.route("/jobs", methods = ['POST', 'GET', 'DELETE'])
def jobs():
    pass

# curl -X GET http://127.0.0.1:5000/jobs/<jobid>
@app.route("/jobs/<job_id>", methods=['GET'])
def get_job(job_id:str):
    pass

# curl -X GET http://127.0.0.1:5000/help
@app.route("/help", methods=['GET'])
def get_help():
    pass

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)


