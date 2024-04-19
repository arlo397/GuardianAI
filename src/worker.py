from jobs import json, logging, logging_level, format_str, rd, q, update_job_status, get_job_by_id, save_job_result

logging.basicConfig(filename='logger.log', format=format_str, level=logging_level, filemode='w')

def execute_job(job_id):
    # Create plot with matplotlib
    pass

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
    job_status_output = execute_job(job_id)

    if job_status_output is True:
        # Update Job Status
        update_job_status(job_id, 'Completed')
    else: 
        update_job_status(job_id, 'Failure')

# Execute Queue Worker
work()