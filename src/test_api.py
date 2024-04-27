import api
from io import BytesIO
import orjson
import pandas as pd
import pytest
from services import RedisDb
from unittest.mock import call, patch, MagicMock, Mock
import zipfile

example_dataframe = pd.DataFrame({
    'trans_date_trans_time': ['21/06/2020 12:14'],
    'cc_num': [2.29116E+15],
    'merchant': ['fraud_Kirlin and Sons'],
    'category': ['personal_care'],
    'amt': [2.86],
    'first': ['Jeff'],
    'last': ['Elliott'],
    'gender': ['M'],
    'street': ['351 Darlene Green'],
    'city': ['Columbia'],
    'state': ['SC'],
    'zip': [29209],
    'lat': [33.9659],
    'long': [-80.9355],
    'city_pop': [333497],
    'job': ['Mechanical engineer'],
    'dob': ['19/03/1968'],
    'trans_num': ['2da90c7d74bd46a0caf3777415b3ebd3'],
    'unix_time': [1371816865],
    'merch_lat': [33.986391],
    'merch_long': [-81.200714],
    'is_fraud': [0],
  })
example_dataframe.index = [0]
example_dataframe_byte_string = b'{"trans_date_trans_time":"21/06/2020 12:14","cc_num":2291160000000000.0,"merchant":"fraud_Kirlin and Sons","category":"personal_care","amt":2.86,"first":"Jeff","last":"Elliott","gender":"M","street":"351 Darlene Green","city":"Columbia","state":"SC","zip":29209,"lat":33.9659,"long":-80.9355,"city_pop":333497,"job":"Mechanical engineer","dob":"19/03/1968","trans_num":"2da90c7d74bd46a0caf3777415b3ebd3","unix_time":1371816865,"merch_lat":33.986391,"merch_long":-81.200714,"is_fraud":0}'

@patch('api.get_redis')
def test_get_transaction_data_from_redis(mock_get_redis: Mock):
  mock_redis = MagicMock()
  mock_pipe = Mock()
  mock_redis.keys.return_value = [b'0', b'1', b'2']
  mock_pipe.execute.return_value = [b'{"look a key": 1}', b'{"look a key": 2}', b'{"look a key": 3}']
  mock_redis.pipeline.return_value.__enter__.return_value = mock_pipe
  mock_get_redis.return_value = mock_redis
  assert api.get_transaction_data_from_redis() == [
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
  mock_get_redis.assert_any_call(RedisDb.TRANSACTION_DB)
  mock_redis.keys.assert_called_once_with()
  mock_redis.pipeline.assert_called_once_with()
  mock_pipe.get.assert_has_calls([
    call(b'0'),
    call(b'1'),
    call(b'2'),
  ])
  mock_pipe.execute.assert_called_once_with()

@patch.dict('os.environ', {}, clear=True)
@patch('logging.error')
def test_attempt_fetch_transaction_data_from_kaggle_fails_without_login_creds(mock_error):
  assert api._attempt_fetch_transaction_data_from_kaggle() is None
  mock_error.assert_called_once()
  err: OSError = mock_error.mock_calls[0].args[0]
  assert isinstance(err, OSError)
  assert str(err).startswith("Could not find kaggle.json. Make sure it's located in ")
  assert str(err).endswith(".kaggle. Or use the environment method.")

@pytest.mark.parametrize('bad_env', [
  {
    'KAGGLE_USERNAME': 'username',
    'KAGGLE_KEY': 'ohlookafakekagglekey',
    'KAGGLE_OWNER': '',
    'KAGGLE_DATASET': 'dataset',
    'KAGGLE_FILENAME': 'filename',
  },
  {
    'KAGGLE_USERNAME': 'username',
    'KAGGLE_KEY': 'ohlookafakekagglekey',
    'KAGGLE_DATASET': 'dataset',
    'KAGGLE_FILENAME': 'filename',
  },
])
def test_attempt_fetch_transaction_data_from_kaggle_fails_with_bad_env(bad_env):
  with patch.dict('os.environ', bad_env, clear=True):
    with patch('logging.error') as mock_error:
      assert api._attempt_fetch_transaction_data_from_kaggle() is None
      mock_error.assert_called_once_with('Invalid value in Kaggle environment variables. Switching to disk fallback.')

@patch.dict('os.environ', {
    'KAGGLE_USERNAME': 'username',
    'KAGGLE_KEY': 'ohlookafakekagglekey',
    'KAGGLE_OWNER': 'owner',
    'KAGGLE_DATASET': 'dataset',
    'KAGGLE_FILENAME': 'filename',
  }, clear=True)
@patch('api.import_kaggle')
def test_attempt_fetch_transaction_data_from_kaggle_fails_with_bad_kaggle_response(mock_import_kaggle):
  mock_kaggle_api = Mock()
  mock_import_kaggle.return_value = lambda: mock_kaggle_api
  mock_kaggle_download = Mock()
  mock_kaggle_download.status = 500
  mock_kaggle_api.datasets_download_file.return_value = mock_kaggle_download
  assert api._attempt_fetch_transaction_data_from_kaggle() is None
  mock_import_kaggle.assert_called_once_with()
  mock_kaggle_api.authenticate.assert_called_once_with()
  mock_kaggle_api.datasets_download_file.assert_called_once_with('owner', 'dataset', 'filename', _preload_content=False)

@patch.dict('os.environ', {
    'KAGGLE_USERNAME': 'username',
    'KAGGLE_KEY': 'ohlookafakekagglekey',
    'KAGGLE_OWNER': 'owner',
    'KAGGLE_DATASET': 'dataset',
    'KAGGLE_FILENAME': 'filename',
  }, clear=True)
@patch('api.import_kaggle')
def test_attempt_fetch_transaction_data_from_kaggle_fails_with_wrong_file_kaggle_response(mock_import_kaggle):
  mock_kaggle_api = Mock()
  mock_import_kaggle.return_value = lambda: mock_kaggle_api
  # We need to make some zipped csv data like would be returned from Kaggle
  # This is not fun
  csv_data = b",trans_date_trans_time,cc_num,merchant,category,amt,first,last,gender,street,city,state,zip,lat,long,city_pop,job,dob,trans_num,unix_time,merch_lat,merch_long,is_fraud\n0,21/06/2020 12:14,2.29116E+15,fraud_Kirlin and Sons,personal_care,2.86,Jeff,Elliott,M,351 Darlene Green,Columbia,SC,29209,33.9659,-80.9355,333497,Mechanical engineer,19/03/1968,2da90c7d74bd46a0caf3777415b3ebd3,1371816865,33.986391,-81.200714,0"
  buffer = BytesIO()
  with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    zipf.writestr('notthefilename', csv_data)
  mock_kaggle_download = Mock()
  mock_kaggle_download.status = 200
  mock_kaggle_download.data = buffer.getvalue()
  mock_kaggle_api.datasets_download_file.return_value = mock_kaggle_download
  assert api._attempt_fetch_transaction_data_from_kaggle() is None
  mock_import_kaggle.assert_called_once_with()
  mock_kaggle_api.authenticate.assert_called_once_with()
  mock_kaggle_api.datasets_download_file.assert_called_once_with('owner', 'dataset', 'filename', _preload_content=False)

@patch.dict('os.environ', {
    'KAGGLE_USERNAME': 'username',
    'KAGGLE_KEY': 'ohlookafakekagglekey',
    'KAGGLE_OWNER': 'owner',
    'KAGGLE_DATASET': 'dataset',
    'KAGGLE_FILENAME': 'filename',
  }, clear=True)
@patch('api.import_kaggle')
def test_attempt_fetch_transaction_data_from_kaggle_succeeds_with_good_env(mock_import_kaggle):
  mock_kaggle_api = Mock()
  mock_import_kaggle.return_value = lambda: mock_kaggle_api
  # We need to make some zipped csv data like would be returned from Kaggle
  # This is not fun
  csv_data = b",trans_date_trans_time,cc_num,merchant,category,amt,first,last,gender,street,city,state,zip,lat,long,city_pop,job,dob,trans_num,unix_time,merch_lat,merch_long,is_fraud\n0,21/06/2020 12:14,2.29116E+15,fraud_Kirlin and Sons,personal_care,2.86,Jeff,Elliott,M,351 Darlene Green,Columbia,SC,29209,33.9659,-80.9355,333497,Mechanical engineer,19/03/1968,2da90c7d74bd46a0caf3777415b3ebd3,1371816865,33.986391,-81.200714,0"
  buffer = BytesIO()
  with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
    zipf.writestr('filename', csv_data)
  mock_kaggle_download = Mock()
  mock_kaggle_download.status = 200
  mock_kaggle_download.data = buffer.getvalue()
  mock_kaggle_api.datasets_download_file.return_value = mock_kaggle_download
  assert api._attempt_fetch_transaction_data_from_kaggle().equals(example_dataframe)
  mock_import_kaggle.assert_called_once_with()
  mock_kaggle_api.authenticate.assert_called_once_with()
  mock_kaggle_api.datasets_download_file.assert_called_once_with('owner', 'dataset', 'filename', _preload_content=False)

@pytest.mark.parametrize('bad_env', [
  {
    'FALLBACK_DATASET_PATH': '',
  },
  {},
])
def test__attempt_read_transaction_data_from_disk_fails_with_bad_env(bad_env):
  with patch.dict('os.environ', bad_env, clear=True):
    with patch('logging.error') as mock_error:
      assert api._attempt_read_transaction_data_from_disk() is None
      mock_error.assert_called_once_with('No valid path to the dataset specified by env var FALLBACK_DATASET_PATH')

@patch.dict('os.environ', {
    'FALLBACK_DATASET_PATH': 'a file.csv',
  }, clear=True)
@patch('pandas.read_csv')
def test_attempt_read_transaction_data_from_disk_succeeds_with_good_env(mock_read_csv):
  mock_read_csv.return_value = 'a fake df'
  assert api._attempt_read_transaction_data_from_disk() == 'a fake df'
  mock_read_csv.assert_called_once_with('a file.csv', sep=',', index_col=0)

@patch('api.abort', side_effect=Exception)
@patch('api._attempt_read_transaction_data_from_disk')
@patch('api._attempt_fetch_transaction_data_from_kaggle')
def test_load_transaction_data_into_redis_aborts_when_kaggle_and_disk_fail(mock_kaggle_fetch, mock_disk_read, mock_abort):
  mock_kaggle_fetch.return_value = None
  mock_disk_read.return_value = None
  with pytest.raises(Exception):
    api.load_transaction_data_into_redis()
  mock_kaggle_fetch.assert_called_once_with()
  mock_disk_read.assert_called_once_with()
  mock_abort.assert_called_once_with(500, 'Unable to fetch data from Kaggle or from disk.')

@patch('api.get_redis')
@patch('api._attempt_fetch_transaction_data_from_kaggle')
def test_load_transaction_data_into_redis_succeeds_with_kaggle(mock_kaggle_fetch, mock_get_redis):
  mock_kaggle_fetch.return_value = example_dataframe
  mock_redis = MagicMock()
  mock_pipe = Mock()
  mock_redis.pipeline.return_value.__enter__.return_value = mock_pipe
  mock_get_redis.return_value = mock_redis
  assert api.load_transaction_data_into_redis() == ('OK', 200)
  mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
  mock_redis.pipeline.assert_called_once_with()
  mock_pipe.set.assert_called_once_with(0, example_dataframe_byte_string)

@patch('api.get_redis')
@patch('api._attempt_read_transaction_data_from_disk')
@patch('api._attempt_fetch_transaction_data_from_kaggle')
def test_load_transaction_data_into_redis_uses_disk_as_backup(mock_kaggle_fetch, mock_disk_read, mock_get_redis):
  mock_kaggle_fetch.return_value = None
  mock_disk_read.return_value = example_dataframe
  mock_redis = MagicMock()
  mock_pipe = Mock()
  mock_redis.pipeline.return_value.__enter__.return_value = mock_pipe
  mock_get_redis.return_value = mock_redis
  assert api.load_transaction_data_into_redis() == ('OK', 200)
  mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
  mock_redis.pipeline.assert_called_once_with()
  mock_pipe.set.assert_called_once_with(0, example_dataframe_byte_string)

@patch('api.get_redis')
def test_clear_transaction_data_succeeds(mock_get_redis):
  mock_redis = Mock()
  mock_redis.flushdb.return_value = True
  mock_get_redis.return_value = mock_redis
  assert api.clear_transaction_data() == ('OK', 200)
  mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
  mock_redis.flushdb.assert_called_once_with()

@patch('api.abort', side_effect=Exception)
@patch('api.get_redis')
def test_clear_transaction_data_calls_abort_on_failure(mock_get_redis, mock_abort):
  mock_redis = Mock()
  mock_redis.flushdb.return_value = False
  mock_get_redis.return_value = mock_redis
  with pytest.raises(Exception):
    api.clear_transaction_data()
  mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
  mock_redis.flushdb.assert_called_once_with()
  mock_abort.assert_called_once_with(500, 'Error clearing data from Redis.')

@pytest.mark.parametrize('badarg,abortmatcher,should_check_redis', [
  ('?limit=a', 'Optional limit parameter must be a valid positive integer.', False),
  ('?offset=a', 'Optional offset parameter must be a valid nonnegative integer.', False),
  ('?offset=5000', 'Optional offset parameter must be less than the length of the dataset.', True),
  ('?limit=0', 'Optional limit parameter must be greater than zero.', True),
])
def test_get_transaction_data_view_calls_abort_on_bad_args(badarg: str, abortmatcher: str, should_check_redis: bool):
  with patch('api.get_redis') as mock_get_redis:
    mock_redis = Mock()
    mock_redis.dbsize.return_value = 10
    mock_get_redis.return_value = mock_redis
    with patch('api.abort', side_effect=Exception) as mock_abort:
      with api.app.test_request_context(badarg):
        with pytest.raises(Exception):
          api.get_transaction_data_view()
      mock_abort.assert_called_once_with(400, abortmatcher)
    if should_check_redis:
      mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
      mock_redis.dbsize.assert_called_once_with()

@pytest.mark.parametrize('arg,expected_start_idx,expected_end_idx', [
  ('', 0, 5),
  ('?limit=2', 0, 2),
  ('?offset=2', 2, 7),
  ('?limit=3&offset=3', 3, 6),
])
def test_get_transaction_data_view_succeeds_with_good_args(arg: str, expected_start_idx: int, expected_end_idx: int):
  fake_data_rows = [{'fake data point': idx} for idx in range(10)]
  with patch('api.get_redis') as mock_get_redis:
    mock_redis = Mock()
    mock_redis.dbsize.return_value = 10
    mock_redis.get.side_effect = lambda idx: orjson.dumps(fake_data_rows[idx])
    mock_get_redis.return_value = mock_redis
    with api.app.test_request_context(arg):
      assert api.get_transaction_data_view() == fake_data_rows[expected_start_idx:expected_end_idx]
    for idx in range(expected_start_idx, expected_end_idx):
      mock_redis.get.assert_any_call(idx)