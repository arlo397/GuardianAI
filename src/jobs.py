import json
import os
import logging
import redis
from socket import gethostname
from hotqueue import HotQueue

logging_level = os.getenv('LOG_LEVEL')
format_str = f'[%(asctime)s {gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s'
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

def _save_job(jid, job_dict):
    """Save a job object in the Redis database."""
    jdb.set(jid, json.dumps(job_dict))
    return

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
        job_dict['status'] = status
        logging.info(f"Job Status Updated for {jid}. Job marked as " + status + ". \n")
        _save_job(jid, job_dict)
    else:
        raise Exception()