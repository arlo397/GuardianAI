import pytest
import services
from unittest.mock import patch, Mock

@pytest.fixture(autouse=True)
def clean_global_state():
  services._redis = None
  services._queue = None

@patch.dict('os.environ', {}, clear=True)
def test_init_backend_services_handles_undefined_redis_ip():
  with pytest.raises(Exception, match='No IP found for Redis. Fix by setting the environment variable REDIS_IP.'):
    services.init_backend_services()

@patch.dict('os.environ', {'REDIS_IP': 'redis'}, clear=True)
@patch('services.sleep')
@patch('services.HotQueue')
@patch('services.Redis')
def test_init_backend_services_inits_redis_and_hotqueue(mock_redis, mock_hotqueue, mock_sleep):
  fake_redis_obj = Mock()
  fake_redis_obj.info = lambda : {'loading': 1}
  mock_redis.return_value = fake_redis_obj
  mock_hotqueue.return_value = Mock()
  def on_sleep_called(_):
    fake_redis_obj.info = lambda : {'loading': 0}
  mock_sleep.side_effect = on_sleep_called
  assert services._redis is None
  assert services._queue is None
  services.init_backend_services()
  assert services._redis is not None
  assert services._queue is not None
  mock_redis.assert_called_once_with(host='redis')
  mock_hotqueue.assert_called_once_with(services.REDIS_JOB_QUEUE_KEY, host='redis', db=services.RedisDb.QUEUE_DB.value)
  mock_sleep.assert_called_once_with(.1)

@pytest.mark.parametrize('db', [db for db in services.RedisDb])
def test_get_redis_handles_unitialized_redis(db):
  assert services.get_redis(db) is None
  mock_none = Mock()
  assert services.get_redis(db, none_handler=mock_none) is None
  mock_none.assert_called_once_with()

@pytest.mark.parametrize('db', [db for db in services.RedisDb])
def test_get_redis_handles_initialized_redis(db):
  services._redis = Mock()
  assert services.get_redis(db) is services._redis
  services._redis.select.assert_called_once_with(db.value)

def test_get_queue_handles_unitialized_queue():
  assert services.get_queue() is None
  mock_none = Mock()
  assert services.get_queue(none_handler=mock_none) is None
  mock_none.assert_called_once_with()

def test_get_queue_handles_initialized_queue():
  services._queue = Mock()
  assert services.get_queue() is services._queue

@patch.dict('os.environ', {}, clear=True)
def test_get_log_level_handles_undefined_env_var():
  with pytest.raises(Exception, match='LOG_LVL not defined in environment variables.'):
    services.get_log_level()

@patch.dict('os.environ', {'LOG_LEVEL': 'nonsense'}, clear=True)
def test_get_log_level_handles_invalid_env_var():
  with pytest.raises(Exception, match='LOG_LVL not defined in environment variables.'):
    services.get_log_level()

@patch.dict('os.environ', {'LOG_LEVEL': 'CRITICAL'}, clear=True)
def test_get_log_level_handles_valid_env_var():
  assert services.get_log_level() == 'CRITICAL'