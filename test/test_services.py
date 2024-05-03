import pytest
import services
from unittest.mock import call, patch, MagicMock, Mock

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
  with pytest.raises(Exception, match=f'{services.LOG_LVL_VAR} invalid or not defined in environment variables.'):
    services.get_log_level()

@patch.dict('os.environ', {services.LOG_LVL_VAR: 'nonsense'}, clear=True)
def test_get_log_level_handles_invalid_env_var():
  with pytest.raises(Exception, match=f'{services.LOG_LVL_VAR} invalid or not defined in environment variables.'):
    services.get_log_level()

@patch.dict('os.environ', {services.LOG_LVL_VAR: 'CRITICAL'}, clear=True)
def test_get_log_level_handles_valid_env_var():
  assert services.get_log_level() == 'CRITICAL'

@patch.dict('os.environ', {}, clear=True)
def test_get_bing_api_key_handles_undefined_env_var():
  with pytest.raises(Exception, match=f'{services.BING_API_KEY_VAR} not defined in environment variables.'):
    services.get_bing_api_key()

@patch.dict('os.environ', {services.BING_API_KEY_VAR: 'apikey1234'}, clear=True)
def test_get_bing_api_key_handles_valid_env_var():
  assert services.get_bing_api_key() == 'apikey1234'

def test_pipeline_data_out_of_redis():
  mock_redis = MagicMock()
  mock_pipe = Mock()
  mock_redis.keys.return_value = [b'0', b'1', b'2']
  mock_pipe.execute.return_value = [b'{"look a key": 1}', b'{"look a key": 2}', b'{"look a key": 3}']
  mock_redis.pipeline.return_value.__enter__.return_value = mock_pipe
  assert services.pipeline_data_out_of_redis(mock_redis) == [
    {
      'look a key': 1,
    },
    {
      'look a key': 2,
    },
    {
      'look a key': 3,
    },
  ]
  mock_redis.keys.assert_called_once_with()
  mock_redis.pipeline.assert_called_once_with()
  mock_pipe.get.assert_has_calls([
    call(b'0'),
    call(b'1'),
    call(b'2'),
  ])
  mock_pipe.execute.assert_called_once_with()

@pytest.mark.parametrize('datestr,expect', [
  ('1/2/3 13:52', False),
  ('01/02/03 13:52', False),
  ('a', False),
  ('a/b/c 13:52', False),
  ('01/02/2024 00:00', True),
  ('01/02/2024 24:00', False),
  ('01/10/202564 12:34', False),
  ('01/10/2025 17:89', False),
  ('01/10/2025 25:30', False),
])
def test_is_valid_date(datestr: str, expect: bool):
  assert services._is_valid_date(datestr) == expect

@pytest.mark.parametrize('client_submitted_data,expected_error_message', [
  ({'transactions': [None]}, 'JSON param "transactions" must be a list of objects.'),
  ({'transactions': [1, 2]}, 'JSON param "transactions" must be a list of objects.'),
  ({'transactions': ['f', {}, {}]}, 'JSON param "transactions" must be a list of objects.'),
  ({'transactions': ['a', 'b']}, 'JSON param "transactions" must be a list of objects.'),
  ({'transactions': [{}]}, 'JSON param "transactions" has object missing key trans_date_trans_time.'),
  ({'transactions': [{
    'trans_date_trans_time': 'hmm',
    'merchant': 'amerchant',
    'category': 'acategory',
    'amt': 1.23,
    'lat': 4.56,
    'long': 7.89,
    'job': 'painter',
    'merch_lat': 7.0,
  }]}, 'JSON param "transactions" has object missing key merch_long.'),
  ({'transactions': [{
    'trans_date_trans_time': 'hmm',
    'merchant': 'amerchant',
    'category': 'acategory',
    'amt': 1.23,
    'lat': 4.56,
    'long': 7.89,
    'job': 'painter',
    'merch_lat': 7.0,
    'merch_long': 'uhohwrongtype',
  }]}, 'JSON param "transactions" has object with key merch_long of incorrect type. (Should be <class \'float\'>).'),
  ({'transactions': [{
    'trans_date_trans_time': 'hmm',
    'merchant': 'amerchant',
    'category': 'acategory',
    'amt': 1.23,
    'lat': 4.56,
    'long': 7.89,
    'job': 'painter',
    'merch_lat': 7.0,
    'merch_long': 7.0,
    'anextrakey': 'rip',
  }]}, 'JSON param "transactions" has an object with too many keys.'),
  ({'transactions': [{
    'trans_date_trans_time': '21/06/2020 12:167',
    'merchant': 'amerchant',
    'category': 'acategory',
    'amt': 1.23,
    'lat': 4.56,
    'long': 7.89,
    'job': 'painter',
    'merch_lat': 7.0,
    'merch_long': 7.0,
  }]}, f'JSON param "transactions" has an object with trans_date_trans_time in invalid format. (Should be {services.TRANSACTION_DATE_TIME_FORMAT}.)'),
  ({'transactions': [{
    'trans_date_trans_time': '21/06/2020 12:16',
    'merchant': 'amerchant',
    'category': 'acategory',
    'amt': 1.23,
    'lat': 4.56,
    'long': 7.89,
    'job': 'painter',
    'merch_lat': 7.0,
    'merch_long': 7.0,
  }]}, None)
])
def test_validate_transaction_list(client_submitted_data, expected_error_message):
  assert services.validate_transaction_list(client_submitted_data) == expected_error_message