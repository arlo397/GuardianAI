from services import PLOTTING_DATA_COLS, PLOTTING_DATA_COLS_NAMES, RedisDb, get_log_level, get_queue, get_redis, init_backend_services
from typing import Any

import orjson
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import logging
import socket
import time
import warnings

warnings.filterwarnings('ignore')
sns.color_palette("pastel")
sns.set_palette("pastel")

# Tried adding timer here so that init_backend_services() would have initialized queue already
time.sleep(20)
q = get_queue()

def get_transaction_data_from_redis() -> list[dict[str, Any]]:
    """
    Returns all the data currently stored in Redis.
    This will be an empty list if there is no data in Redis.

    Returns:
        result (list[dict[str, Any]]): The data stored in Redis.
    """
    keys = get_redis(RedisDb.TRANSACTION_DB).keys()
    with get_redis(RedisDb.TRANSACTION_DB).pipeline() as pipe:
        for key in keys: pipe.get(key)
        data = pipe.execute()
    return [orjson.loads(d) for d in data]

def execute_job(job_id, job_description_dict:dict, labels=['Not Fraud','Fraud']):
    # Get Plot Independent Variable from Job Dictionary
    independent_variable = job_description_dict["graph_feature"]
    
    # Check if worker is compatible to plot feature
    if independent_variable not in PLOTTING_DATA_COLS: 
        return ("Unavailable metric to plot. \n")
    
    data = get_transaction_data_from_redis()
    
    if not data:
        return "No data available. \n", 404
 
    df = pd.DataFrame(data)
    df[['trans_date', 'trans_time']] = df['trans_date_trans_time'].str.split(' ', expand=True)
    df['trans_date_trans_time'] = pd.to_datetime(df['trans_date_trans_time'])
    df['trans_date'] = pd.to_datetime(df['trans_date'])
    df['trans_time'] = pd.to_datetime(df['trans_time'], format= '%H:%M').dt.time

    df['trans_month'] = df['trans_date'].dt.to_period('M').astype("str")
    df['trans_dayOfWeek'] = df['trans_date'].dt.day_name()
    df['fraud'] = df['is_fraud'].apply(lambda x: "Fraud" if x == 1 else 'Not Fraud')
    
    # Get Column Name from Independent Variable 
    # Get the index of an element
    index = PLOTTING_DATA_COLS.index(independent_variable)
    independent_variable_str = PLOTTING_DATA_COLS_NAMES[index]
    
    fig, axes = plt.subplots(1, 2, figsize=(10, 6)) # (width, height)
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
            return("Error. This should not happen. \n")

    # Save Plot briefly in container before you move it to Redis
    plt.savefig(f'/job_{job_id}_output.png') 

    # Save Plot into Redis Results Data Base
    try:
        with open(f'/job_{job_id}_output.png', 'rb') as f:
            img = f.read()
        
        successful_data_entry = get_redis(RedisDb.JOB_RESULTS_DB).hset(job_id, 'image', img)
    
    except FileNotFoundError:
        # Handle the case where the file doesn't exist
        print(f"Error: File '/job_{job_id}_output.png' not found")

    except IOError as e:
        # Handle general I/O errors
        print(f"Error: I/O error occurred - {e}")

    except Exception as e:
        # Catch any other unexpected exceptions and handle them appropriately
        print(f"An unexpected error occurred: {e}")
        raise

    except:
        raise Exception
    
    if successful_data_entry == 1:  
        return True
    else: 
        return False
    
@q.worker
def work(job_id):
    """Worker pull items off the queue to execute jobs for client. 
    Args:
        job_id (int): Unique identifier for Job stored in database
    """
    logging.info("Popping job off of queue ... ")
    # Get JOB Description
    job_description_dict = get_redis(RedisDb.JOB_DB).get(id)
    
    if job_description_dict is None:
        return ('Invalid job id. \n')

    # Update Job Status
    get_redis(RedisDb.JOB_RESULTS_DB).set(job_id, orjson.dumps({
            'status': 'In Progress',
            'graph_feature': job_description_dict['graph_feature'],
        }))

    # Start Job
    job_status_output = execute_job(job_id, job_description_dict)

    if job_status_output is True:
        # Update Job Status
        get_redis(RedisDb.JOB_RESULTS_DB).set(job_id, orjson.dumps({
            'status': 'completed',
            'graph_feature': job_description_dict['graph_feature'],
        }))
    else: 
        # Update Job Status
        get_redis(RedisDb.JOB_RESULTS_DB).set(job_id, orjson.dumps({
            'status': 'failure',
            'graph_feature': job_description_dict['graph_feature'],
        }))

def main():
    logging.info('Credit Card Fraud Transaction Worker service started')
    init_backend_services()
    logging.info('Redis and HotQueue instances attached, beginning work...')
    work()

if __name__ == '__main__':
    logging.basicConfig(
        format=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s',
        level=get_log_level())
    main()
