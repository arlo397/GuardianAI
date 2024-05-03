from csv import reader
from datetime import datetime
from hotqueue import HotQueue
from input_vectorization import flatten, onehot_encode
from io import BytesIO
import logging
import matplotlib.pyplot as plt
from ml_model import load_saved_model
import orjson
import pandas as pd
from redis import Redis
import seaborn as sns
from services import PLOTTING_DATA_COLS, PLOTTING_DATA_COLS_NAMES, RedisDb, get_log_level, get_queue, get_redis as generic_get_redis, init_backend_services, pipeline_data_out_of_redis, validate_transaction_list
import socket
import torch
from typing import Any
import warnings

def _on_no_queue():
    """Raise an exception when get_queue is called before HotQueue is initialized.

    Raises:
        Exception: If HotQueue hasn't been initialized yet.
    """
    logging.error('get_queue called before HotQueue was initialized.')
    raise Exception("HotQueue hasn't been initialized yet.")

def _on_no_redis():
    """Raise an exception when get_redis is called before Redis is initialized.

    Raises:
        Exception: If Redis hasn't been initialized yet.
    """
    logging.error('get_redis called before Redis was initialized.')
    raise Exception("Redis hasn't been initialized yet.")

def get_redis(db: RedisDb) -> Redis:
    """
    Gets Redis and raises an Exception if it hasn't been initialized yet.

    Returns:
        redis (Redis): The Redis instance.
    """
    return generic_get_redis(db, none_handler=_on_no_redis)

def _begin_job(job_id) -> dict[str, Any]:
    """
    Fetches the info about a job stored in Redis by the job ID and updates the job to in progress
    Throws an error if the job info is not found in Redis

    Args:
        job_id (str): The ID of the job to find and update in Redis
    Returns:
        result (dict[str, Any]): The updated info of the job from Redis
    """
    job_info = get_redis(RedisDb.JOB_DB).get(job_id)
    if not job_info:
        raise Exception('Job not found in Redis.')
    job_info = orjson.loads(job_info)
    job_info['status'] = 'in_progress'
    get_redis(RedisDb.JOB_DB).set(job_id, orjson.dumps(job_info))
    return job_info

def _execute_graph_feature_analysis_job(job_id: str, job_description_dict: dict[str, str]) -> bool:
    """
    Attempts to perform mathematical analysis requested in the job description.
    The result is an image plot saved as a binary string in Redis.
    
    Arguments:
        job_id (str): The ID of the job
        job_description_dict (dict[str, str]): The job information
    Returns:
        result (bool): Whether or not the job was completed successfully
    """
    labels=['Legitimate', 'Fraudulent']
    independent_variable = job_description_dict["graph_feature"]
    
    # Check if worker is compatible to plot feature
    if independent_variable not in PLOTTING_DATA_COLS: 
        logging.error(f'JOB ID: {job_id} | Unavailable metric to plot.')
        return False

    df = pd.DataFrame(pipeline_data_out_of_redis(get_redis(RedisDb.TRANSACTION_DB)))
    df[['trans_date', 'trans_time']] = df['trans_date_trans_time'].str.split(' ', expand=True)
    df['trans_date'] = pd.to_datetime(df['trans_date'], format='%m/%d/%Y')

    df['trans_month'] = df['trans_date'].dt.to_period('M').astype('str')
    df['trans_dayOfWeek'] = df['trans_date'].dt.day_name()
    df['fraud'] = df['is_fraud'].apply(lambda x: 'Fraudulent' if x == 1 else 'Legitimate')
    
    independent_variable_str = PLOTTING_DATA_COLS_NAMES[PLOTTING_DATA_COLS.index(independent_variable)]
    
    _, axes = plt.subplots(1, 2, figsize=(10, 6))
    plt.suptitle(f'Distribution of Transaction by {independent_variable_str}', fontsize=20, fontweight='bold')
    
    for i, ax in enumerate(axes.flatten()):
        df_1 = df[df['is_fraud'] == i]
        
        if independent_variable == 'trans_month':
            ax = df_1.groupby(independent_variable)['amt'].sum().plot(kind='bar', ax=ax, label='Count')
            ax.set_xticklabels(ax.get_xticklabels(), rotation = 45)
            ax.set_ylabel('Count')
            ax.legend(loc='upper left')
            
            ax1 = ax.twinx()
            ax1 = df_1.groupby(independent_variable).size().plot(kind='line',color='orange', label='Amount', ax=ax1)
            ax1.set_xticklabels(ax.get_xticklabels(), rotation = 45)
            ax1.set_ylabel('Amount ($)')  
            ax1.set_title(f"{labels[i]}")
            ax1.legend(loc='upper right')

        elif independent_variable == 'gender':
            ax.pie(df_1[independent_variable].value_counts(), labels = ['Female','Male'] , autopct='%1.1f%%')
            ax.set_title(f"{labels[i]}")

        elif independent_variable == 'trans_dayOfWeek':
            cats = [ 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            ax = sns.barplot(data = df_1.groupby(independent_variable).size().reset_index(), x = independent_variable, y=0, label = 'Count'
                            , color='#a1c9f4', order=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'], ax=ax)
            ax.set_ylabel('Count')
            ax.set_xticklabels(cats, rotation=45)  # Set x-axis tick labels to the days of the week
            ax.legend(loc='upper left')

            ax1 = ax.twinx()
            ax1 = sns.lineplot(data = df_1.groupby(independent_variable)['amt'].sum().reindex(cats).reset_index(), x = independent_variable, y='amt', label ='Amount', color='orange', ax=ax1)
            ax1.set_ylabel('Amount ($)')
            ax1.set_title(f"{labels[i]}")
            ax1.legend(loc='upper right')
        
        elif independent_variable == 'category':
            cats = df_1['category'].unique().tolist()  # Get unique transaction categories
            ax = sns.barplot(data = df_1.groupby(independent_variable).size().reset_index(), x = independent_variable, y=0, label = 'Count'
                            ,color='#a1c9f4', order=cats, ax=ax)
            ax.set_ylabel('Count')
            ax.set_xticklabels(cats, rotation=90)
            ax.legend(loc='upper left')
            ax.set_title(f"{labels[i]}")
        else:
            logging.error(f'JOB ID: {job_id} | Encountered unsupported data element despite checking PLOTTING_DATA_COLS.')
            return False

    # Save Plot into Redis Results Data Base
    try:
        img_buffer = BytesIO()
        plt.savefig(img_buffer, format='png')
        successful_data_entry = get_redis(RedisDb.JOB_RESULTS_DB).set(job_id, img_buffer.getvalue())
        if not successful_data_entry:
            raise Exception('Image data failed to be set in Redis.')
    except Exception as e:
        logging.error(e)
        return False
    return True

def _extract_row(t: dict[str, Any]) -> list[str|float|int]:
    """
    Converts a transaction dictionary from a transaction analysis job input to a row of values.
    This is the first step in preparing a tensor for model inference.
    The date is broken into more meaningful numeric components.

    Arguments:
        t (dict[str, Any]): The transaction dictionary
    Returns:
        result (list[str|float|int]): The transaction dictionary as a list of values
    """
    date = datetime.strptime(t['trans_date_trans_time'], '%d/%m/%Y %H:%M')
    return [date.day, date.month, date.year, date.weekday(), date.hour, date.minute, t['merchant'], t['category'], t['amt'], t['lat'], t['long'], t['job'], t['merch_lat'], t['merch_long']]

def _standardize_tensor(tensor: torch.Tensor) -> torch.Tensor:
    """
    This is the last step in preparing a tensor for model inference.
    This loads the mean and standard deviation tensors (obtained from training data)
    from saved files and then standardizes the input tensor with them.

    This function requires a 'meanandstd.txt' file to be present in the
    current working directory with the first line being the mean tensor values
    and the second line being the std tensor values, e.g.
    1.0, 2.0, 3.0
    0.1, 0.2, 0.3

    Arguments:
        tensor (torch.Tensor): The tensor to standardize
    Returns:
        result (torch.Tensor): The standardized tensor
    """
    with open('meanandstd.txt', 'r') as meanandstd_file:
        lines = [line.strip().split(', ') for line in meanandstd_file.readlines()[:2]]
    mean = torch.tensor([float(m) for m in lines[0]], dtype=torch.float)
    std = torch.tensor([float(s) for s in lines[1]], dtype=torch.float)
    return (tensor - mean) / (std + 1e-8)

def _execute_transaction_analysis_job(job_id: str, job_info: dict[str, Any]) -> bool:
    """
    Attempts to perform model inference requested in the job description.
    The result is a list of classifications (either zeros to indicate legitimate
    transactions or ones to indicate fraudulent ones).
    
    Arguments:
        job_id (str): The ID of the job
        job_info (dict[str, Any]): The job information
    Returns:
        result (bool): Whether or not the job was completed successfully
    """
    try:
        if isinstance(job_info['transactions'], list) and job_info['transactions']:
            err = validate_transaction_list(job_info)
            if err is None:
                with open('merchants.txt', 'r') as merchants_file:
                    merchants = [line.strip() for line in merchants_file.readlines()]
                with open('categories.txt', 'r') as categories_file:
                    categories = [line.strip() for line in categories_file.readlines()]
                with open('jobs.txt', 'r') as jobs_file:
                    jobs = [line.strip() for line in jobs_file.readlines()]
                transaction_rows = [_extract_row(t) for t in job_info['transactions']]
                transaction_rows = [flatten(onehot_encode(t, merchants, categories, jobs)) for t in transaction_rows]
                inputs = _standardize_tensor(torch.tensor(transaction_rows, dtype=torch.float))
                model = load_saved_model('binaryclassifierstate.pt')
                model.eval()
                with torch.no_grad():
                    predictions = model(inputs).round().squeeze()
                if predictions.dim() == 0:
                    predictions = [predictions.item()]
                else:
                    predictions = [p.item() for p in predictions]
                successful_data_entry = get_redis(RedisDb.JOB_RESULTS_DB).set(job_id, orjson.dumps(predictions))
                if not successful_data_entry:
                    logging.error('Failed to upload results to Redis.')
                    return False
                return True
            logging.error(err)
        return False
    except Exception as e:
        logging.error(e)
        return False

def _execute_job(job_id: str, job_info: dict[str, Any]) -> bool:
    """
    Performs a job - either analysis of a graph feature or use of an ML model to predict
    if a group of transactions are fraudulent or not.

    Args:
        job_id (str): uuid of submitted job
        job_description_dict (dict): Job information dictionary specifying the desired graph parameter.

    Returns:
        boolean: Whether or not the job was executed successfully.
    """
    if 'graph_feature' in job_info:
        return _execute_graph_feature_analysis_job(job_id, job_info)
    if 'transactions' in job_info:
        return _execute_transaction_analysis_job(job_id, job_info)
    logging.error(f'Malformed job: {job_info}')
    return False

def _complete_job(job_id: str, job_info: dict[str, Any], success: bool):
    """Updates job status to complete, either successfully or in failure. 

    Args:
        job_id (str): uuid of job
        job_info (dict[str, Any]): Job information dictionary that will be updated
        success (bool): Boolean indicating whether the job was successfully completed based on Redis hset function
    """
    job_info['status'] = 'completed' if success else 'failed'
    logging.info(f'Job {job_id} updated information: {job_info}')
    get_redis(RedisDb.JOB_DB).set(job_id, orjson.dumps(job_info))
    logging.info(f'Job information updated in database...')

def do_jobs(queue: HotQueue):
    """Starts a worker to execute jobs from the provided HotQueue.

    Args:
        queue (HotQueue): The HotQueue instance, or None if queue is None
    """
    @queue.worker
    def do_job(job_id: str):
        """Pops job tasks off of queue to execute

        Args:
            job_id (str): uuid of job
        """
        try:
            job_info = _begin_job(job_id)
            logging.info("Job has begun...")
            success = _execute_job(job_id, job_info)
            logging.info(f"Job has finished executing. {job_id} success code is {success}.")
            _complete_job(job_id, job_info, success)
        except Exception as e:
            logging.error(e)
    do_job()


def main():
    logging.info('Credit Card Fraud Transaction Worker service started')
    warnings.filterwarnings('ignore')
    sns.color_palette("pastel")
    sns.set_palette("pastel")
    init_backend_services()
    logging.info('Redis and HotQueue instances attached, beginning work...')
    do_jobs(get_queue(none_handler=_on_no_queue))


if __name__ == '__main__':
    logging.basicConfig(
        format=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s',
        level=get_log_level())
    main()
