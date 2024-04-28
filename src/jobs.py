from socket import gethostname
from hotqueue import HotQueue
from services import get_log_level
from api import *

import json
import os
import logging
import redis
import socket

format_str = f'[%(asctime)s {gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s'
logging.basicConfig(format=f'[%(asctime)s {socket.gethostname()}] %(filename)s:%(funcName)s:%(lineno)s - %(levelname)s: %(message)s', level=get_log_level())

def _save_job(jid, job_dict):
    """Save a job object in the Redis database."""
    get_redis(RedisDb.JOB_DB).set(jid, orjson.dumps(job_dict))
    return 

def get_job_by_id(jid:str):
    """Return job dictionary given jid"""
    return get_redis(RedisDb.JOB_DB).get(jid)

def get_all_job_ids() -> list[str]:
    """Return list of Job ids"""
    return [j.decode('utf-8') for j in get_redis(RedisDb.JOB_DB).lrange(REDIS_JOB_IDS_KEY, 0, -1)]

def delete_all_jobs():
    """Deletes all jobs from jobs database"""
    if get_redis(RedisDb.JOB_DB).flushdb(): return 'OK', 200
    abort(500, 'Error flushing jobs db.')

def update_job_status(jid:str, status:str):
    """Update the status of job with job id `jid` to status `status`."""
    job_dict = get_job_by_id(jid)
    if job_dict:
        job_dict['status'] = status
        logging.info(f"Job Status Updated for {jid}. Job marked as " + status + ". \n")
        _save_job(jid, job_dict)
    else:
        raise Exception()