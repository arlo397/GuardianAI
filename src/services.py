from datetime import datetime
from enum import Enum
from hotqueue import HotQueue
import logging
from orjson import loads
from os import environ
from redis import Redis
from time import sleep
from typing import Any, Callable, Optional

_redis: Optional[Redis] = None
_queue: Optional[HotQueue] = None

BING_API_KEY_VAR = 'BING_API_KEY'
LOG_LVL_VAR = 'LOG_LEVEL'
REDIS_IP_VAR = 'REDIS_IP'
REDIS_JOB_QUEUE_KEY = 'job_queue'
REDIS_JOB_IDS_KEY = 'job_ids'
TRANSACTION_DATE_TIME_FORMAT = '%d/%m/%Y %H:%M'

OK_200 = ('OK\n', 200)

class RedisDb(Enum):
  TRANSACTION_DB = 0
  QUEUE_DB = 1
  JOB_DB = 2
  JOB_RESULTS_DB = 3

PLOTTING_DATA_COLS =  ['trans_month','trans_dayOfWeek','gender','category']
PLOTTING_DATA_COLS_NAMES = ['Month','Day of Week','Gender','Transaction Category']

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
  _queue = HotQueue(REDIS_JOB_QUEUE_KEY, host=redis_addr, db=RedisDb.QUEUE_DB.value)

def get_redis(db: int, none_handler: Optional[Callable[[], None]] = None) -> Redis:
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
    log_lvl (str): The log level from the environment.
  """
  log_lvl = environ.get(LOG_LVL_VAR)
  if log_lvl in logging._levelToName.values():
    return log_lvl
  raise Exception(f'{LOG_LVL_VAR} invalid or not defined in environment variables.')

def get_bing_api_key() -> str:
  """
  Retrieves the Bing API key from the environment using BING_API_KEY_VAR.
  Throws an Exception if the bing API key is not found in the environment.

  Returns:
    bing_api_key (str): The Bing API key from the environment.
  """
  bing_api_key = environ.get(BING_API_KEY_VAR)
  if bing_api_key is not None:
    return bing_api_key
  raise Exception(f'{BING_API_KEY_VAR} not defined in environment variables.')

def pipeline_data_out_of_redis(redisdb: Redis) -> list[dict[str, Any]]:
  """
    Returns all the data currently stored in Redis.
    This will be an empty list if there is no data in Redis.

    Returns:
        result (list[dict[str, Any]]): The data stored in Redis.
    """
  keys = redisdb.keys()
  with redisdb.pipeline() as pipe:
    for key in keys: pipe.get(key)
    data = pipe.execute()
  return [loads(d) for d in data]

def _is_valid_date(date_string: str):
  try:
    datetime.strptime(date_string, TRANSACTION_DATE_TIME_FORMAT)
    return True
  except ValueError:
    return False

def validate_transaction_list(client_submitted_data: dict[str, list[Any]]) -> str | None:
  required_keys_and_types = {
    'trans_date_trans_time': str,
    'merchant': str,
    'category': str,
    'amt': float,
    'lat': float,
    'long': float,
    'job': str,
    'merch_lat': float,
    'merch_long': float,
  }
  for t in client_submitted_data['transactions']:
    if not isinstance(t, dict): return 'JSON param "transactions" must be a list of objects.'
    for k, v in required_keys_and_types.items():
        if k not in t: return f'JSON param "transactions" has object missing key {k}.'
        if not isinstance(t[k], v): return f'JSON param "transactions" has object with key {k} of incorrect type. (Should be {v}).'
    if len(t) > len(required_keys_and_types): return 'JSON param "transactions" has an object with too many keys.'
    if not _is_valid_date(t['trans_date_trans_time']): return f'JSON param "transactions" has an object with trans_date_trans_time in invalid format. (Should be {TRANSACTION_DATE_TIME_FORMAT}.)'