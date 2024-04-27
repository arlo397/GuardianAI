from enum import Enum
from hotqueue import HotQueue
import logging
from os import environ
from redis import Redis
from time import sleep
from typing import Callable, Optional

_redis: Optional[Redis] = None
_queue: Optional[HotQueue] = None

LOG_LVL_VAR = 'LOG_LEVEL'
REDIS_IP_VAR = 'REDIS_IP'
REDIS_JOB_QUEUE_KEY = 'job_queue'
REDIS_JOB_IDS_KEY = 'job_ids'
class RedisDb(Enum):
  TRANSACTION_DB = 0
  QUEUE_DB = 1
  JOB_DB = 2
  JOB_RESULTS_DB = 3

ALL_DATA_COLS = [
    'trans_date_trans_time', 'cc_num','merchant', 'category', 'amt', 'first', 'last',
    'gender', 'street', 'city', 'state', 'zip', 'lat', 'long', 'city_pop', 'job', 'dob',
    'trans_num', 'unix_time', 'merch_lat', 'merch_long', 'is_fraud',
]

def init_backend_services():
  """
  Initializes the Redis and HotQueue instances so that get_redis and get_queue function correctly.
  """
  redis_addr = environ.get(REDIS_IP_VAR)
  if redis_addr is None:
    raise Exception('No IP found for Redis. Fix by setting the environment variable REDIS_IP.')
  global _redis
  global _queue
  _redis = Redis(host=redis_addr)
  waits = 0
  while True:
    info = _redis.info()
    if info['loading'] == 0:
      logging.info('Redis initialized!')
      break
    if waits % 10 == 0:
      logging.info('Waiting for Redis to initialize...')
    sleep(0.1)
    waits += 1
  _queue = HotQueue(REDIS_JOB_QUEUE_KEY, host=redis_addr, db=RedisDb.TRANSACTION_DB.value)

def get_redis(db: RedisDb, none_handler: Optional[Callable[[], None]] = None) -> Redis:
  """
  Returns the Redis instance.
  Runs the none_handler and returns None if Redis hasn't been initialized yet.

  Args:
    db (int): Optional kwarg to select which db redis is set to, defaults to GENE_DB
    none_handler (Optional[Callable[[], None]]): Optional kwarg to select a function to be called in case Redis is None
  Returns:
    redis (Redis): The redis instance with the selected db, or None if redis is None
  """
  global _redis
  if _redis is None:
    if none_handler: none_handler()
    return None
  _redis.select(db.value)
  return _redis

def get_queue(none_handler: Optional[Callable[[], None]] = None) -> HotQueue:
  """
  Returns the HotQueue instance.
  Runs the none_handler and returns None if HotQueue hasn't been initialized yet.

  Args:
    none_handler (Optional[Callable[[], None]]): Optional kwarg to select a function to be called in case the Queue is None
  Returns:
    queue (HotQueue): The HotQueue instance, or None if queue is None
  """
  global _queue
  if _queue is None:
    if none_handler: none_handler()
    return None
  return _queue

def get_log_level() -> str:
  """
  Retrieves the log level from the environment using LOG_LVL_VAR.
  Throws an Exception if the log level is not found in the environment.

  Returns:
    log_lvl (str): The log level from the environment
  """
  log_lvl = environ.get(LOG_LVL_VAR)
  if log_lvl in logging._levelToName.values():
    return log_lvl
  raise Exception('LOG_LVL not defined in environment variables.')