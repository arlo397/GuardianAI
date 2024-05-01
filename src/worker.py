from services import PLOTTING_DATA_COLS, PLOTTING_DATA_COLS_NAMES, RedisDb, get_log_level, get_queue, get_redis as generic_get_redis, init_backend_services, pipeline_data_out_of_redis

from hotqueue import HotQueue
from io import BytesIO
import logging
import matplotlib.pyplot as plt
import orjson
import pandas as pd
from redis import Redis
import seaborn as sns
import socket
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

def _execute_job(job_id, job_description_dict: dict[str, str]) -> bool:
    """Generates and saves matplotlib graph based on the user input feature when submitting a job. 

    Args:
        job_id (str): uuid of submitted job
        job_description_dict (dict): Job information dictionary specifying the desired graph parameter. 

    Raises:
        Exception: Raises all possible exceptions that may be raised while storing and reading image. 

    Returns:
        boolean: Boolean specifying whether the image was properly generated and saved. 
    """
    labels=['Not Fraud','Fraud']
    # Get Plot Independent Variable from Job Dictionary
    independent_variable = job_description_dict["graph_feature"]
    
    # Check if worker is compatible to plot feature
    if independent_variable not in PLOTTING_DATA_COLS: 
        logging.error(f'JOB ID: {job_id} | Unavailable metric to plot.')
        return False
    
    data = pipeline_data_out_of_redis(get_redis(RedisDb.TRANSACTION_DB))

    df = pd.DataFrame(data)
    df[['trans_date', 'trans_time']] = df['trans_date_trans_time'].str.split(' ', expand=True)
    # df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    df['trans_date'] = pd.to_datetime(df['trans_date'], format='mixed', dayfirst=True)
    # df['trans_time'] = pd.to_datetime(df['trans_time'], format= '%H:%M').dt.time

    df['trans_month'] = df['trans_date'].dt.to_period('M').astype("str")
    df['trans_dayOfWeek'] = df['trans_date'].dt.day_name()
    df['fraud'] = df['is_fraud'].apply(lambda x: "Fraud" if x == 1 else 'Not Fraud')
    
    # Get Column Name from Independent Variable 
    # Get the index of an element
    index = PLOTTING_DATA_COLS.index(independent_variable)
    independent_variable_str = PLOTTING_DATA_COLS_NAMES[index]
    
    _, axes = plt.subplots(1, 2, figsize=(10, 6)) # (width, height)
    plt.suptitle("Distribution of Transaction by " + independent_variable_str, fontsize=20, fontweight='bold')
    
    for i, ax in enumerate(axes.flatten()):
        df_1 = df[df['is_fraud'] == i]
        
        if (independent_variable == 'trans_month'):
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

        elif (independent_variable == 'gender'):
            ax.pie(df_1[independent_variable].value_counts(), labels = ['Female','Male'] , autopct='%1.1f%%')
            ax.set_title(f"{labels[i]}")

        elif (independent_variable == 'trans_dayOfWeek'):
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
