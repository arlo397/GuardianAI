from flask import Flask, send_file, abort, request
from hotqueue import HotQueue
from io import BytesIO
import logging
import orjson
from os import environ
import pandas as pd
from redis import Redis
import requests
from services import OK_200, PLOTTING_DATA_COLS, REDIS_JOB_IDS_KEY, RedisDb, get_log_level, init_backend_services, \
      get_queue as generic_get_queue, get_redis as generic_get_redis, pipeline_data_out_of_redis
import socket
from typing import Any, Optional
import urllib3
from uuid import uuid4
import zipfile

BING_API_KEY = "AoaZqu_awoToijquulNRBaNbW98dniWa17O-QGrlBxP6Nv60C-3YaMIDkLqNb5UL"

app = Flask(__name__)

queue_none_handler = lambda: abort(500, 'Unable to interact with jobs - HotQueue not initialized.')
redis_none_handler = lambda: abort(500, 'Unable to read/write interact with data - Redis not initialized.')

def import_kaggle():
    """Imports KaggleApi from the kaggle module.

    Returns:
        KaggleApi: An instance of KaggleApi class for interacting with the Kaggle API.
    """
    from kaggle import KaggleApi
    return KaggleApi


def get_queue() -> HotQueue:
    """
    Gets the HotQueue and raises a 500 error if it hasn't been initialized yet.

    Returns:
        queue (HotQueue): The HotQueue instance.
    """
    return generic_get_queue(none_handler=queue_none_handler)


def get_redis(db: RedisDb) -> Redis:
    """
    Gets Redis and raises a 500 error if it hasn't been initialized yet.

    Returns:
        redis (Redis): The Redis instance.
    """
    return generic_get_redis(db, none_handler=redis_none_handler)


# curl localhost:5173/transaction_data
@app.route('/transaction_data')
def get_transaction_data_from_redis() -> list[dict[str, Any]]:
    """
    Returns all the data currently stored in Redis.
    This will be an empty list if there is no data in Redis.

    Returns:
        result (list[dict[str, Any]]): The data stored in Redis.
    """
    return pipeline_data_out_of_redis(get_redis(RedisDb.TRANSACTION_DB))


def _attempt_fetch_transaction_data_from_kaggle() -> Optional[pd.DataFrame]:
    """
    Tries to fetch the data from Kaggle. The following environment variables are required.
    KAGGLE_USERNAME, KAGGLE_KEY, KAGGLE_OWNER, KAGGLE_DATASET, KAGGLE_FILENAME. If a var
    is not present, or is invalid, or Kaggle authentication fails, or there's an error
    downloading or processing the data into a dataframe, the exception will be logged
    appropriately and None will be returned. If the data is loaded successfully, the
    resulting pd.DataFrame will be returned.
    
    Returns:
        result (Optional[pd.DataFrame]): A pd.DataFrame if successful, otherwise None.
    """
    try:
        # Delaying this import because __init__ tries to authenticate and will throw an error
        # if we import it at the top and the Kaggle creds aren't present.
        KaggleApi = import_kaggle()
        kaggle_vars = [environ.get(var) for var in ['KAGGLE_OWNER', 'KAGGLE_DATASET', 'KAGGLE_FILENAME']]
        if '' in kaggle_vars or None in kaggle_vars:
            logging.error('Invalid value in Kaggle environment variables. Switching to disk fallback.')
            return None
        client = KaggleApi()
        logging.debug('Authenticating with Kaggle...')
        client.authenticate()
        logging.debug('Authenticated, fetching data...')
        result: urllib3.response.HTTPResponse = client.datasets_download_file(*kaggle_vars, _preload_content=False)
        assert result.status == 200
        logging.debug('Response received from Kaggle, downloading and unzipping...')
        buffer = BytesIO(result.data)
        with zipfile.ZipFile(buffer, 'r') as zip_file:
            extracted_files = {name: zip_file.read(name) for name in zip_file.namelist()}
        logging.debug('Unzipped Kaggle data.')
        if kaggle_vars[2] not in extracted_files:
            logging.error('File not found in Kaggle download result.')
            return None
        filepath_or_data = BytesIO(extracted_files[kaggle_vars[2]])
        logging.debug('CSV data pulled into memory from Kaggle.')
        df = pd.read_csv(filepath_or_data, sep=',')
        logging.debug('Pandas DataFrame generated.')
        return df
    except Exception as e:
        logging.error(e)
        return None


def _attempt_read_transaction_data_from_disk() -> Optional[pd.DataFrame]:
    """
    Tries to read the data from disk at the path specified by the FALLBACK_DATASET_PATH
    environment variable. If the environment variable isn't present or is invalid,
    or if the data at the path is invalid, the appropriate error logging will take place
    and None will be returned. Otherwise, the processed pd.DataFrame will be returned.

    Returns:
        result (Optional[pd.DataFrame]): A pd.DataFrame if successful, otherwise None.
    """
    try:
        dataset_path = environ.get('FALLBACK_DATASET_PATH')
        if dataset_path in ['', None]:
            logging.error('No valid path to the dataset specified by env var FALLBACK_DATASET_PATH')
            return None
        logging.debug('Dataset fallback path identified, attempting to read in the data...')
        df = pd.read_csv(dataset_path, sep=',')
        logging.debug('Pandas DataFrame generated.')
        return df
    except Exception as e:
        logging.error(e)
        return None


# curl -X POST localhost:5173/transaction_data
@app.route('/transaction_data', methods=['POST'])
def load_transaction_data_into_redis() -> tuple[str, int]:
    """
    Fetches the data from Kaggle or from disk as specified by the environment variable.
    If environment vars KAGGLE_USERNAME, KAGGLE_KEY, KAGGLE_OWNER, KAGGLE_DATASET,
    and KAGGLE_FILENAME are all provided, the route will attempt to load data using the
    Kaggle API. If these are not present, or if there's an exception, the route will
    attempt to load the data from a csv file on disk at the location specified by the
    environment variable FALLBACK_DATASET_PATH.

    To be clear, either
    (KAGGLE_USER, KAGGLE_PWD, KAGGLE_OWNER, KAGGLE_DATASET, KAGGLE_FILENAME) OR
    (FALLBACK_DATASET_PATH)
    MUST be provided.

    If the data cannot be loaded via either method, a 500 error is returned.

    Returns:
        HTTP Result (tuple[str, int]): A 200 OK or a 500 and corresponding error message / explanation.
    """
    df = _attempt_fetch_transaction_data_from_kaggle()
    if df is None:
        df = _attempt_read_transaction_data_from_disk()
    if df is None:
        abort(500, 'Unable to fetch data from Kaggle or from disk.')
    data = df.to_dict(orient='records')

    with get_redis(RedisDb.TRANSACTION_DB).pipeline() as pipe:
        for idx, record in enumerate(data):
            pipe.set(idx, orjson.dumps(record))
        pipe.execute()
    logging.info('Data POSTED into Redis Database.')
    return OK_200


# curl -X DELETE localhost:5173/transaction_data
@app.route('/transaction_data', methods=['DELETE'])
def clear_transaction_data() -> tuple[str, int]:
    """
    DELETEs transaction fraud data from Redis database

    Returns:
        str: Confirmation about API task executed
        List: List of dictionaries for each data observation
    """
    if get_redis(RedisDb.TRANSACTION_DB).flushdb():
        logging.info('Data DELETED from Redis Database.')
        return OK_200
    abort(500, 'Error clearing data from Redis.')


# curl "localhost:5173/transaction_data_view?limit=2&offset=7"
# curl localhost:5173/transaction_data_view
@app.route('/transaction_data_view')
def get_transaction_data_view() -> list[dict[str, Any]]:
    """
    Returns a slice of the data in redis.
    Optional query params are 'limit' and 'offset'
    If provided, must be valid positive integers
    Offset defaults to zero
    Limit defaults to the entire set
    Invalid parameters result in a 400 Bad request.
    Error fetching or processing data results in a 500 Internal server error.

    Returns:
        result (list[dict[str, Any]]): [the data entries as a list of dictionaries / JSON objects]
    """
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    if limit is not None and not limit.isnumeric():
        abort(400, 'Optional limit parameter must be a valid positive integer.')
    if offset is not None and not offset.isnumeric():
        abort(400, 'Optional offset parameter must be a valid nonnegative integer.')
    if offset is None:
        offset = 0
    else:
        offset = int(offset)
    if offset >= get_redis(RedisDb.TRANSACTION_DB).dbsize():
        abort(400, 'Optional offset parameter must be less than the length of the dataset.')
    if limit is None:
        limit = 5
    else:
        limit = int(limit)
    if limit == 0:
        abort(400, 'Optional limit parameter must be greater than zero.')
    return [orjson.loads(get_redis(RedisDb.TRANSACTION_DB).get(trans_id)) for trans_id in range(offset, offset + limit)]


# curl localhost:5173/amt_analysis
@app.route('/amt_analysis')
def amt_analysis() -> dict[str, float]:
    """
    Computes and returns statistical descriptions of the transaction amounts in the dataset.
    Returns this as a dict. Fails with abort and appropriate error code and message if the
    data hasn't been loaded into Redis yet or if there's an error computing the statistics.

    Returns:
        result (dict[str, float]): A dict containing statistical summaries of the 'amt' field
        in the dataset, including count, mean, std, min, 25%, 50%, 75%, and max.
    """
    data = get_transaction_data_from_redis()
    if not data:
        abort(400, 'Data must be loaded into Redis before analysis can be performed.')
    try:
        df = pd.DataFrame(data)
        amt_stats = df['amt'].describe()
        stats_dict = amt_stats.to_dict()
        return stats_dict
    except Exception as e:
        logging.error(f'Error computing statistics: {e}')
        abort(500, 'Error computing statistics.')


# curl localhost:5173/amt_fraud_correlation
@app.route('/amt_fraud_correlation')
def compute_correlation() -> dict[str, dict[str, float]]:
    """
    Computes the correlation between transaction amount ('amt') and fraud status ('is_fraud') in the dataset.

    Returns:
        result (dict[str, dict[str, float]]) A dict containing the correlation matrix between 'amt' and 'is_fraud'.
    """
    data = get_transaction_data_from_redis()
    if not data:
        abort(400, 'Data must be loaded into Redis before analysis can be performed.')
    try:
        df = pd.DataFrame(data)
        if 'amt' not in df.columns or 'is_fraud' not in df.columns:
            logging.error('Required columns are missing.')
            abort(500, 'Required columns are missing from the dataset.')
        correlation = df[['amt', 'is_fraud']].corr()
        return correlation.to_dict()
    except Exception as e:
        logging.error(f'Error computing correlation: {e}')
        abort(500, 'Error computing correlation.')


# curl localhost:5173/fraudulent_zipcode_info
@app.route('/fraudulent_zipcode_info')
def fraudulent_zipcode_info() -> dict[str, str | float]:
    """
    Identifies the zipcode with the highest number of fraudulent transactions, and retrieves its geographic location.

    Returns:
        result (dict[str, str|float]): A dict containing the most fraudulent zipcode, the number of frauds, and a Google Maps link to the location.
    """
    data = get_transaction_data_from_redis()
    if not data:
        abort(400, 'Data must be loaded into Redis before analysis can be performed.')
    try:
        df = pd.DataFrame(data)
        fraud_transactions = df[df['is_fraud'] == 1]
        fraudulent_zipcode_counts = fraud_transactions['zip'].astype(str).value_counts()

        most_fraudulent_zipcode = fraudulent_zipcode_counts.idxmax()
        max_fraud_count = fraudulent_zipcode_counts.max()

        response = requests.get(
            f'http://dev.virtualearth.net/REST/v1/Locations/US/{most_fraudulent_zipcode}',
            params={'key': BING_API_KEY}
        )
        if response.status_code == 200 and response.json() is not None and response.json()['resourceSets'][0][
            'resources']:
            location_data = data['resourceSets'][0]['resources'][0]
            lat, lon = location_data['point']['coordinates']
            google_maps_link = f'https://www.google.com/maps/search/?api=1&query={lat},{lon}'

            return {
                'most_fraudulent_zipcode': most_fraudulent_zipcode,
                'fraud_count': max_fraud_count,
                'latitude': lat,
                'longitude': lon,
                'Google Maps Link': google_maps_link
            }
        else:
            abort(500, 'Location not found for the most fraudulent zipcode.')
    except Exception as e:
        logging.error(f'Error omputing statistics or fetching location: {e}')
        abort(500, 'Error computing statistics or fetching location.')


# curl localhost:5173/fraud_by_state
@app.route('/fraud_by_state')
def fraud_by_state() -> dict[str, int]:
    """
    Returns the number of fraudulent transactions per state.

    Returns:
        result (dict[str, int]): A dict containing the count of fraudulent transactions by state.
    """
    data = get_transaction_data_from_redis()
    if not data:
        abort(400, 'Data must be loaded into Redis before analysis can be performed.')
    try:
        df = pd.DataFrame(data)
        # Check if required columns are present
        if 'state' not in df.columns or 'is_fraud' not in df.columns:
            logging.error('Required columns are missing.')
            abort(500, 'Required columns are missing from the dataset.')

        # Filter fraudulent transactions and count by state
        fraud_transactions = df[df['is_fraud'] == 1]
        fraud_counts_by_state = fraud_transactions['state'].value_counts().to_dict()

        return fraud_counts_by_state

    except Exception as e:
        logging.error(f'Error processing data: {e}')
        abort(500, 'Error processing data.')


# curl localhost:5173/ai_analysis
@app.route('/ai_analysis')
def ai_analysis() -> dict:
    """
    Endpoint for analyzing the feature importance of a trained model.
    This function calls the train_model function to get the trained model
    and its feature importances, then returns those importances in a JSON format.

    Returns:
        Flask Response: JSON formatted string of feature importances sorted by their importance.
        If there's an error during processing, an internal server error is raised.
    """
    try:
        # Import the function to train the model and get feature importances
        from ML_model import train_model

        # Train the model and retrieve feature importances
        feature_importances = train_model()

        # Convert the DataFrame of feature importances to JSON format
        # Ensure the DataFrame is returned as a JSON response by using 'records' orientation
        return feature_importances.to_dict(orient='records')

    except Exception as e:
        # Log any errors encountered during the process
        logging.error(f'Error processing data: {e}')

        # Abort the request and return an HTTP 500 Internal Server Error response
        # The description provides more context about the error
        abort(500, 'Error processing data.')


# curl localhost:5173/jobs
@app.route('/jobs')
def get_all_existing_job_ids() -> list[str]:
    """
    Returns all job ids in the database.
    Gives a 500 error if Redis hasn't been initialized yet.

    Returns:
        result (list[str]): A list containing all of the job ids.
    """
    return [j.decode('utf-8') for j in get_redis(RedisDb.JOB_DB).lrange(REDIS_JOB_IDS_KEY, 0, -1)]


# curl -X DELETE localhost:5173/jobs 
@app.route('/jobs', methods=['DELETE'])
def clear_all_jobs() -> tuple[str, int]:
    """
    Clears all jobs from the jobs database.
    Gives a 500 error if Redis hasn't been initialized yet.

    Returns:
        result (tuple[str, int]): A OK 200 or error message and 500.
    """
    if get_redis(RedisDb.JOB_DB).flushdb(): return OK_200
    abort(500, 'Error flushing jobs db.')


# curl -X POST localhost:5173/jobs -d '{"graph_feature": "gender"}' -H "Content-Type: application/json"
# curl -X POST localhost:5173/jobs -d '{"graph_feature": "trans_month"}' -H "Content-Type: application/json"
# curl -X POST localhost:5173/jobs -d '{"graph_feature": "trans_dayOfWeek"}' -H "Content-Type: application/json"
# curl -X POST localhost:5173/jobs -d '{"graph_feature": "category"}' -H "Content-Type: application/json"
@app.route('/jobs', methods=['POST'])
def post_job() -> dict[str, str]:
    """
    Ensures that valid JSON params were sent with the JOB POST request.
    Then creates a unique job id, saves the job information, queues the job,
    and returns the job id.
    If the request is invalid an error message with code 400 will be returned.

    Returns:
        result (dict[str, str]): The job id in JSON format as {"job_id": job_id}
    """
    client_submitted_data = request.get_json(silent=True)
    if client_submitted_data:
        if isinstance(client_submitted_data, dict) and len(
                client_submitted_data) == 1 and 'graph_feature' in client_submitted_data:
            if client_submitted_data['graph_feature'] in PLOTTING_DATA_COLS:
                job_id = str(uuid4())
                get_redis(RedisDb.JOB_DB).set(job_id, orjson.dumps({
                    'status': 'queued',
                    'graph_feature': client_submitted_data['graph_feature'],
                }))
                get_redis(RedisDb.JOB_DB).rpush(REDIS_JOB_IDS_KEY, job_id)
                get_queue().put(job_id)
                return {'job_id': job_id}
            abort(400, f'JSON param "graph_feature" must be included in {PLOTTING_DATA_COLS}')
        abort(400, 'JSON data params must be an object with a single key: "graph_feature".')
    abort(400,
            'JSON data params must be delivered in the body with the POST request. Param details are specified in the README file.')


# curl http://127.0.0.1:5173/jobs/<id>
@app.route('/jobs/<id>')
def get_job_information(id: str) -> dict[str, str]:
    """
    Returns information about the specified job id.
    Gives a 500 error if Redis hasn't been initialized yet.

    Args:
        id (str): The job ID to search Redis for.
    Returns:
        result (dict[str, Any]): The queried job information.
    """
    job_info = get_redis(RedisDb.JOB_DB).get(id)
    if job_info is None:
        abort(400, 'Invalid job id.')
    return orjson.loads(job_info)


# curl http://127.0.0.1:5173/results/<id>
@app.route('/results/<id>')
def get_job_result(id: str) -> Any:
    """
    Returns the job result as a image file download.
    Gives a 400 bad request error if the job hasn't been completed yet.
    Also errors per get_job_information spec.

    Args:
        id (str): The job ID to get the result of.
    Returns:
        result (Any): The png file download.
    """
    job_info = get_job_information(id)
    if 'status' in job_info:
        if job_info['status'] == 'completed':
            result = get_redis(RedisDb.JOB_RESULTS_DB).get(id)
            if result is None:
                logging.error(f'Job marked as completed but no result found in DB. Job no {id}')
                abort(500, 'Job marked as completed but no result found in DB.')
            return send_file(BytesIO(result), mimetype='image/png', as_attachment=True,
                            download_name=f'plot {id}.png')
        abort(400, 'Job is not complete.')
    logging.error(f'Job {id} is malformed. {job_info}')
    abort(500, 'Malformed job.')


# curl http://127.0.0.1:5173/help
@app.route('/help')
def get_help():
    """Returns instructions for user to utlize the endpoints.

    Returns:
        str: Help string explaining all endpoints. 
    """
    # Define descriptions for each endpoint
    endpoints = {
        '/transaction_data (GET)': {
            'description': 'Returns all transaction data currently stored in Redis.',
            'example_curl': 'curl http://127.0.0.1:5173/transaction_data'
        },
        '/transaction_data (POST)': {
            'description': 'Fetches transaction data from Kaggle or disk and stores it in Redis.',
            'example_curl': 'curl -X POST localhost:5173/transaction_data'
        },
        '/transaction_data (DELETE)': {
            'description': 'Deletes all transaction data stored in Redis.',
            'example_curl': 'curl -X DELETE localhost:5173/transaction_data'
        },
        '/transaction_data_view': {
            'description': 'Returns a default slice of the transaction data stored in Redis (first 5 entries).',
            'example_curl': 'curl localhost:5173/transaction_data_view'
        },
         '/transaction_data_view?limit=<int>&offset=<int>': {
            'description': 'Returns a slice of the transaction data stored in Redis.',
            'example_curl': 'curl "localhost:5173/transaction_data_view?limit=2&offset=7"'
        },
         '/amt_analysis': {
            'description': 'Returns statistical descriptions of the transaction amounts in the dataset.',
            'example_curl': 'curl "localhost:5173/amt_analysis"'
        },
        '/amt_fraud_correlation':{
            'description': 'Returns the correlation between transaction amount and fraud status in the dataset.',
            'example_curl': 'curl "localhost:5173/amt_fraud_correlation"'
        }, 
         '/fraudulent_zipcode_info':{
            'description': 'Returns the zipcode with the highest number of fraudulent transactions, and retrieves its geographic location.',
            'example_curl': 'curl "localhost:5173/fraudulent_zipcode_info"'
        }, 
         '/fraud_by_state':{
            'description': ' Returns the number of fraudulent transactions per state.',
            'example_curl': 'curl "localhost:5173/fraud_by_state"'
        }, 
         '/ai_analysis':{
            'description': 'Returns the most important features and feature importances from the trained model.',
            'example_curl': 'curl "localhost:5173/ai_analysis"'
        }, 
         '/jobs (GET)':{
            'description': 'Returns all job ids in the database.',
            'example_curl': 'curl "localhost:5173/jobs"'
        }, 
         '/jobs (DELETE)':{
            'description': 'Clears all jobs from the jobs database.',
            'example_curl': 'curl -X DELETE "localhost:5173/jobs"'
        }, 
        '/jobs (POST)': {
            'description': 'Creates a job for plotting a feature specified by the user.',
            'Graph Feature Parameters': ["gender", "trans_month", "trans_dayOfWeek", "category"],
            'example_curl': 'curl -X POST localhost:5173/jobs -d "{\"graph_feature\": \"gender\"}" -H "Content-Type: application/json"'
        },
        '/jobs/<id>' :{
            'description': 'Returns information about the specified job id.',
            'example_curl': 'curl -X DELETE "localhost:5173/jobs/99e6820f-0e4f-4b55-8052-7845ea390a44"'
        },
        '/results/<id>' :{
            'description': ' Returns the job result as a image file download.',
            'example_curl': 'curl -X DELETE "localhost:5173/results/99e6820f-0e4f-4b55-8052-7845ea390a44"'
        },
    }

    output_string = 'Description of all application routes:\n'
    # Print each endpoint's information in a single line
    for endpoint, info in endpoints.items():
        output_string += f"{endpoint}: {info['description']}\n   Example Command: {info['example_curl']}\n"  
        output_string += "\n"

    return output_string

def main():
    """
    Initializes Redis, Hotqueue, and runs the Flask app.
    Assumes you are running in the environment established by docker-compose up.
    """
    logging.info('Credit Card Fraud Transaction API service started')
    init_backend_services()
    logging.info('Redis and HotQueue instances attached, serving clients...')
    app.run(debug=True, host='0.0.0.0', port=5173)


if __name__ == '__main__':
    logging.basicConfig(
        format=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s',
        level=get_log_level())
    main()