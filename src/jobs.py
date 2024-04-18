import json
import os
import logging
import uuid
import redis
from hotqueue import HotQueue

logging_level = os.getenv('LOG_LEVEL')
format_str = f'[%(asctime)s]:%(levelname)s: %(message)s'
logging.basicConfig(filename='logger.log', format=format_str, level=logging.DEBUG, filemode='w')

_redis_ip = os.getenv('REDIS_IP')
_redis_port = 6379


try: 
    rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)      # Raw Data Database
    q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1) # Job Ids in Queue to be consumed by worker
    jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)     # Jobs Database storing Job information
    resdb = redis.Redis(host=_redis_ip, port=_redis_port, db=3)   # Results Database storing results from worker
except Exception as e: 
    logging.info("Connect Exception has occured while trying to connect to the databases")
    rd = None
    q = None
    jdb = None
    resdb = None

def _generate_jid():
    """Generate a pseudo-random identifier for a job."""
    return str(uuid.uuid4())

def _instantiate_job(jid, status, param1, param2):
    return({
                'id': jid,
                'Status': status,
                '': param1,
                '': param2 
            })

def _save_job(jid, job_dict):
    """Save a job object in the Redis database."""
    jdb.set(jid, json.dumps(job_dict))
    return

def _queue_job(jid:str):
    """Add a job to the redis queue."""
    q.put(jid)
    return

def add_job(start, end, status="Submitted"):
    """Add a job to the redis queue."""
    jid:str = _generate_jid()
    job_dict = _instantiate_job(jid, status, start, end)
    _save_job(jid, job_dict)
    _queue_job(jid)
    return job_dict

def get_job_by_id(jid:str):
    """Return job dictionary given jid"""
    return json.loads(jdb.get(jid))

def get_all_job_ids():
    """Return list of Job ids"""
    job_ids = []
    for id in jdb.keys():
       job_ids.append(id.decode())
    return job_ids

def update_job_status(jid:str, status:str):
    """Update the status of job with job id `jid` to status `status`."""
    job_dict = get_job_by_id(jid)
    if job_dict:
        job_dict['Status'] = status
        logging.info(f"Job Status Updated for {jid}. Job marked as " + status + ". \n")
        _save_job(jid, job_dict)
    else:
        raise Exception()
    
def save_job_result(jid:str, result:dict):
    successful_data_entry = resdb.set(jid, json.dumps(result))
    logging.info("Job result stored to database. \n")
    return successful_data_entry

def get_job_result(jid:str):
    return json.loads(resdb.get(jid))

def delete_all_jobs():
    """Function deletes all jobs stored in jdb
    Returns:
        int: 0 is returned indicating successful deletion of jobs in the data base
    """
    for job_id in jdb.keys():
        jdb.delete(job_id)
    logging.info(f"All jobs deleted. \n")
    return 0