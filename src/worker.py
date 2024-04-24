from jobs import logging, logging_level, format_str, q, update_job_status, get_job_by_id, save_job_result
from api import get_data

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings('ignore')
sns.color_palette("pastel")
sns.set_palette("pastel")

logging.basicConfig(filename='logger.log', format=format_str, level=logging_level, filemode='w')

columns = ['trans_month','trans_dayOfWeek','gender','category']
columns_name = ['Month','Day of Week','Gender','Transaction Category']
labels = ['Not Fraud','Fraud']

def execute_job(job_id, columns, columns_name, labels):
    # Get JOB Description
    job_description_dict = get_job_by_id(job_id)

    independent_variable = job_description_dict["Graph Feature"]
    
    # Get Plot Independent Variable from Job Dictionary
    if independent_variable not in columns: 
        return ("Unavailable metric to plot. \n")
    
    data = get_data()
    if not data:
        logging.error("No data available. \n")
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
    index = columns.index(independent_variable)
    independent_variable_str = columns_name[index]
    
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
            return("Error. \n")

    # Save Plot briefly in container before you move it to Redis
    plt.savefig(f'/job_{job_id}_output.png') 

    # Save Plot into Redis Results Data Base
    try:
        with open(f'/job_{job_id}_output.png', 'rb') as f:
            img = f.read()
        successful_data_entry = save_job_result(job_id, img)
    except:
        raise Exception
    
    if successful_data_entry is True: 
        logging.info("Job finished executing with success. \n")
        return True
    else: 
        logging.warning("Job failed to finish with success. \n")
        return False
    
@q.worker
def work(job_id):
    """Worker pull items off the queue to execute jobs for client. 
    Args:
        job_id (int): Unique identifier for Job stored in database
    """
    # Decorator pops job off from queue 

    # Update Job Status
    update_job_status(job_id, 'In Progress')

    # Start Job
    job_status_output = execute_job(job_id, columns, columns_name, labels)

    if job_status_output is True:
        # Update Job Status
        update_job_status(job_id, 'Completed')
    else: 
        update_job_status(job_id, 'Failure')

# Execute Queue Worker
work()