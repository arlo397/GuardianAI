import api
from io import BytesIO
import orjson
import pandas as pd
import pytest
from services import OK_200, PLOTTING_DATA_COLS, RedisDb, REDIS_JOB_IDS_KEY, TRANSACTION_DATE_TIME_FORMAT
from typing import Any
from unittest.mock import patch, MagicMock, Mock
from werkzeug.exceptions import HTTPException
import zipfile

example_dataframe = pd.DataFrame({
    'Unnamed: 0': [0],
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
example_dataframe_byte_string = b'{"Unnamed: 0":0,"trans_date_trans_time":"21/06/2020 12:14","cc_num":2291160000000000.0,"merchant":"fraud_Kirlin and Sons","category":"personal_care","amt":2.86,"first":"Jeff","last":"Elliott","gender":"M","street":"351 Darlene Green","city":"Columbia","state":"SC","zip":29209,"lat":33.9659,"long":-80.9355,"city_pop":333497,"job":"Mechanical engineer","dob":"19/03/1968","trans_num":"2da90c7d74bd46a0caf3777415b3ebd3","unix_time":1371816865,"merch_lat":33.986391,"merch_long":-81.200714,"is_fraud":0}'

@patch('api.pipeline_data_out_of_redis')
@patch('api.get_redis')
def test_get_transaction_data_from_redis(mock_get_redis, mock_pipeline_data_out_of_redis):
  mock_get_redis.return_value = 'aredisinstance'
  mock_pipeline_data_out_of_redis.return_value = 'apipelinedataoutofredisreturnvalue'
  assert api.get_transaction_data_from_redis() == 'apipelinedataoutofredisreturnvalue'
  mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
  mock_pipeline_data_out_of_redis.assert_called_once_with('aredisinstance')

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
def test_attempt_read_transaction_data_from_disk_fails_with_bad_env(bad_env):
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
  mock_read_csv.assert_called_once_with('a file.csv', sep=',')

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
  assert api.load_transaction_data_into_redis() == OK_200
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
  assert api.load_transaction_data_into_redis() == OK_200
  mock_get_redis.assert_called_once_with(RedisDb.TRANSACTION_DB)
  mock_redis.pipeline.assert_called_once_with()
  mock_pipe.set.assert_called_once_with(0, example_dataframe_byte_string)

@patch('api.get_redis')
def test_clear_transaction_data_succeeds(mock_get_redis):
  mock_redis = Mock()
  mock_redis.flushdb.return_value = True
  mock_get_redis.return_value = mock_redis
  assert api.clear_transaction_data() == OK_200
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

def test_AnalysisManager_init():
  am = api.AnalysisManager(['required_col'])
  assert am.required_cols == ['required_col']

@pytest.mark.parametrize('data', [None, []])
def test_AnalysisManager_enter_fails_on_bad_data(data):
  with patch('api.get_transaction_data_from_redis') as mock_get_transaction_data_from_redis:
    mock_get_transaction_data_from_redis.return_value = data
    with patch('api.abort', side_effect=Exception) as mock_abort:
      am = api.AnalysisManager(['col1', 'col2'])
      with pytest.raises(Exception):
        am.__enter__()
      mock_abort.assert_called_once_with(400, 'Data must be loaded into Redis before analysis can be performed.')
    mock_get_transaction_data_from_redis.assert_called_once_with()

@patch('api.abort', side_effect=Exception)
@patch('api.get_transaction_data_from_redis')
def test_AnalysisManager_enter_fails_on_missing_cols(mock_get_transaction_data_from_redis, mock_abort):
  mock_get_transaction_data_from_redis.return_value = [
    {
      'col1': 0,
    },
    {
      'col1': 1,
    },
    {
      'col1': 2,
    },
  ]
  am = api.AnalysisManager(['col1', 'col2'])
  with pytest.raises(Exception):
    am.__enter__()
  mock_abort.assert_called_once_with(500, 'Required column col2 is missing from the dataset.')
  mock_get_transaction_data_from_redis.assert_called_once_with()

@patch('api.get_transaction_data_from_redis')
def test_AnalysisManager_enter_drops_unused_cols_and_succeeds(mock_get_transaction_data_from_redis):
  mock_get_transaction_data_from_redis.return_value = [
    {
      'col1': 0,
      'col2': 3,
      'col3': 6,
    },
    {
      'col1': 1,
      'col2': 4,
      'col3': 7,
    },
    {
      'col1': 2,
      'col2': 5,
      'col3': 8,
    },
  ]
  am = api.AnalysisManager(['col1', 'col2'])
  df = am.__enter__()
  mock_get_transaction_data_from_redis.assert_called_once_with()
  assert (df.columns == ['col1', 'col2']).all()

def test_AnalysisManager_exit_propagates_HTTPExceptions():
  am = api.AnalysisManager(['col1', 'col2'])
  ex_type = HTTPException
  ex_val = HTTPException()
  assert not am.__exit__(ex_type, ex_val, None)

def test_AnalysisManager_exit_ignores_None_Exceptions():
  am = api.AnalysisManager(['col1', 'col2'])
  assert am.__exit__(None, None, None)

@pytest.mark.parametrize('exception', [ValueError('some bs value error'), Exception('some other statistics exception')])
def test_AnalysisManager_exit_aborts_on_all_other_exceptions(exception: Exception):
  am = api.AnalysisManager(['col1', 'col2'])
  with patch('api.abort', side_effect=Exception) as mock_abort:
    with pytest.raises(Exception):
      am.__exit__(type(exception), exception, None)
    mock_abort.assert_called_once_with(500, 'Error computing statistics.')

@patch('api.get_transaction_data_from_redis')
def test_amt_analysis(mock_get_transaction_data_from_redis):
  mock_get_transaction_data_from_redis.return_value = [
    {'amt': 1},
    {'amt': 2},
    {'amt': 3},
    {'amt': 4},
    {'amt': 5},
    {'amt': 6},
    {'amt': 7},
    {'amt': 8},
    {'amt': 9},
  ]
  assert api.amt_analysis() == {
    '25%': 3.0,
    '50%': 5.0,
    '75%': 7.0,
    'count': 9.0,
    'max': 9.0,
    'mean': 5.0,
    'min': 1.0,
    'std': 2.7386127875258306,
  }

@patch('api.get_transaction_data_from_redis')
def test_compute_correlation(mock_get_transaction_data_from_redis):
  mock_get_transaction_data_from_redis.return_value = [
    {'amt': 1, 'is_fraud': 0},
    {'amt': 2, 'is_fraud': 0},
    {'amt': 3, 'is_fraud': 0},
    {'amt': 4, 'is_fraud': 0},
    {'amt': 5, 'is_fraud': 1},
    {'amt': 6, 'is_fraud': 0},
    {'amt': 7, 'is_fraud': 0},
    {'amt': 8, 'is_fraud': 0},
    {'amt': 9, 'is_fraud': 0},
  ]
  result: dict[str, dict[str, float]] = api.compute_correlation()
  assert result.keys() == {'amt', 'is_fraud'}
  assert result['amt'].keys() == {'amt', 'is_fraud'}
  assert result['is_fraud'].keys() == {'amt', 'is_fraud'}
  assert isinstance(result['amt']['amt'], float)
  assert isinstance(result['amt']['is_fraud'], float)
  assert isinstance(result['is_fraud']['amt'], float)
  assert isinstance(result['is_fraud']['is_fraud'], float)

fraudulent_zipcode_test_data = [
  {'zip': 11111, 'is_fraud': 0},
  {'zip': 22222, 'is_fraud': 1},
  {'zip': 33333, 'is_fraud': 0},
  {'zip': 44444, 'is_fraud': 0},
  {'zip': 55555, 'is_fraud': 0},
  {'zip': 11111, 'is_fraud': 0},
  {'zip': 22222, 'is_fraud': 1},
  {'zip': 33333, 'is_fraud': 1},
  {'zip': 44444, 'is_fraud': 0},
  {'zip': 55555, 'is_fraud': 0},
]

@patch('api.abort', side_effect=Exception)
@patch('api.get_bing_api_key', side_effect=Exception)
def test_fraudulent_zipcode_info_fails_without_bing_api_key(mock_get_bing_api_key, mock_abort):
  with pytest.raises(Exception):
    api.fraudulent_zipcode_info()
  mock_get_bing_api_key.assert_called_once_with()
  mock_abort.assert_called_once_with(500, 'Error fetching bing api key.')

@patch('api.abort', side_effect=Exception)
@patch('requests.get', side_effect=Exception)
@patch('api.get_transaction_data_from_redis')
@patch('api.get_bing_api_key')
def test_fraudulent_zipcode_info_fails_when_requests_get_fails(
  mock_get_bing_api_key, mock_get_transaction_data_from_redis, mock_requests_get, mock_abort):
  mock_get_bing_api_key.return_value = 'afakeapikey'
  mock_get_transaction_data_from_redis.return_value = fraudulent_zipcode_test_data
  with pytest.raises(Exception):
    api.fraudulent_zipcode_info()
  mock_get_bing_api_key.assert_called_once_with()
  mock_requests_get.assert_called_once_with('http://dev.virtualearth.net/REST/v1/Locations/US/22222', params={'key': 'afakeapikey'})
  mock_abort.assert_called_once_with(500, 'Error fetching location.')

def _fake_json_response(fake_json: Any) -> Mock:
  response = Mock(status_code=200)
  response.json.return_value = fake_json
  return response

@pytest.mark.parametrize('response', [
  None,
  Mock(status_code=0),
  Mock(status_code=200),
  _fake_json_response(None),
  _fake_json_response({}),
  _fake_json_response({'resourceSets': 7}),
  _fake_json_response({'resourceSets': []}),
  _fake_json_response({'resourceSets': {}}),
  _fake_json_response({'resourceSets': [{}]}),
  _fake_json_response({'resourceSets': [{'resources': 'rip'}]}),
  _fake_json_response({'resourceSets': [{'resources': []}]}),
  _fake_json_response({'resourceSets': [{'resources': [None]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': False}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': 5}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': None}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [0.0]}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [0.0, 1.0, 2.0]}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [-90.0, 200.0]}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [-100.0, 90.0]}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [-10.0, 200.0]}}]}]}),
  _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [-90, 90]}}]}]}),
])
def test_fraudulent_zipcode_info_fails_when_response_cannot_parse(response):
  with patch('api.get_bing_api_key') as mock_get_bing_api_key:
    mock_get_bing_api_key.return_value = 'afakeapikey'
    with patch('api.get_transaction_data_from_redis') as mock_get_transaction_data_from_redis:
      mock_get_transaction_data_from_redis.return_value = fraudulent_zipcode_test_data
      with patch('requests.get') as mock_requests_get:
        mock_requests_get.return_value = response
        with patch('api.abort', side_effect=Exception) as mock_abort:
          with pytest.raises(Exception):
            api.fraudulent_zipcode_info()
          mock_abort.assert_called_once_with(500, 'Error parsing virtualearth response.')
        mock_requests_get.assert_called_once_with('http://dev.virtualearth.net/REST/v1/Locations/US/22222', params={'key': 'afakeapikey'})
    mock_get_bing_api_key.assert_called_once_with()

@patch('requests.get')
@patch('api.get_transaction_data_from_redis')
@patch('api.get_bing_api_key')
def test_fraudulent_zipcode_info_succeeds_with_valid_api_key_data_and_response(mock_get_bing_api_key, mock_get_transaction_data_from_redis, mock_requests_get):
  mock_get_bing_api_key.return_value = 'afakeapikey'
  mock_get_transaction_data_from_redis.return_value = fraudulent_zipcode_test_data
  mock_requests_get.return_value = _fake_json_response({'resourceSets': [{'resources': [{'point': {'coordinates': [0.0, 0.0]}}]}]})
  assert api.fraudulent_zipcode_info() == {
    'most_fraudulent_zipcode': '22222',
    'fraud_count': 2,
    'latitude': 0.0,
    'longitude': 0.0,
    'Google Maps Link': 'https://www.google.com/maps/search/?api=1&query=0.0,0.0'
  }
  mock_get_bing_api_key.assert_called_once_with()
  mock_requests_get.assert_called_once_with('http://dev.virtualearth.net/REST/v1/Locations/US/22222', params={'key': 'afakeapikey'})

@patch('api.get_transaction_data_from_redis')
def test_fraud_by_state(mock_get_transaction_data_from_redis):
  mock_get_transaction_data_from_redis.return_value = [
    {'state': 'AZ', 'is_fraud': 0},
    {'state': 'AL', 'is_fraud': 1},
    {'state': 'SC', 'is_fraud': 0},
    {'state': 'CA', 'is_fraud': 1},
    {'state': 'AZ', 'is_fraud': 0},
    {'state': 'IA', 'is_fraud': 1},
    {'state': 'IA', 'is_fraud': 0},
    {'state': 'IA', 'is_fraud': 1},
    {'state': 'NY', 'is_fraud': 0},
    {'state': 'NJ', 'is_fraud': 1},
    {'state': 'TX', 'is_fraud': 0},
  ]
  assert api.fraud_by_state() == {
    'AL': 1,
    'CA': 1,
    'IA': 2,
    'NJ': 1,
  }

@patch('api.get_redis')
def test_get_all_existing_job_ids(mock_get_redis):
  job_ids = [b'0', b'1', b'2', b'3', b'4', b'5']
  mock_redis = Mock()
  mock_redis.lrange.return_value = job_ids
  mock_get_redis.return_value = mock_redis
  assert api.get_all_existing_job_ids() == ['0', '1', '2', '3', '4', '5']
  mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
  mock_redis.lrange.assert_called_once_with(REDIS_JOB_IDS_KEY, 0, -1)

@patch('api.get_redis')
def test_clear_all_jobs_succeeds(mock_get_redis):
  mock_redis = Mock()
  mock_redis.flushdb.return_value = True
  mock_get_redis.return_value = mock_redis
  assert api.clear_all_jobs() == OK_200
  mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
  mock_redis.flushdb.assert_called_once_with()

@patch('api.abort', side_effect=Exception)
@patch('api.get_redis')
def test_clear_all_jobs_calls_abort_on_failure(mock_get_redis, mock_abort):
  mock_redis = Mock()
  mock_redis.flushdb.return_value = False
  mock_get_redis.return_value = mock_redis
  with pytest.raises(Exception):
    api.clear_all_jobs()
  mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
  mock_redis.flushdb.assert_called_once_with()
  mock_abort.assert_called_once_with(500, 'Error flushing jobs db.')

@pytest.mark.parametrize('json,error_message', [
  (None, 'JSON data params must be delivered in the body with the POST request. Param details are specified in the README file.'),
  (['graph_feature'], 'JSON data params must be an object with a single key: "graph_feature" or "transactions".'),
  ({'k': 'v', 'k2': 'v2'}, 'JSON data params must be an object with a single key: "graph_feature" or "transactions".'),
  ({'k': 'v'}, 'JSON data params must be an object with a single key: "graph_feature" or "transactions".'),
  ({'graph_feature': 5}, f'JSON param "graph_feature" must be included in {PLOTTING_DATA_COLS}'),
  ({'graph_feature': 'notavalidone'}, f'JSON param "graph_feature" must be included in {PLOTTING_DATA_COLS}'),
  (['transactions'], 'JSON data params must be an object with a single key: "graph_feature" or "transactions".'),
  ({'transactions': 5}, 'JSON param "transactions" must be a non-empty list of transactions.'),
  ({'transactions': []}, 'JSON param "transactions" must be a non-empty list of transactions.'),
])
def test_post_job_fails_with_appropriate_error_message_on_bad_input(json: Any, error_message: str):
  with api.app.test_request_context(content_type='application/json', json=json):
    with patch('api.abort', side_effect=Exception) as mock_abort:
      with pytest.raises(Exception):
        api.post_job()
      mock_abort.assert_called_once_with(400, error_message)

@patch('api.get_queue')
@patch('api.get_redis')
@patch('api.uuid4')
def test_post_job_succeeds_on_valid_graph_feature_input(mock_uuid, mock_get_redis, mock_get_queue):
  mock_uuid.return_value = 'oohanid'
  mock_redis = Mock()
  mock_queue = Mock()
  mock_get_redis.return_value = mock_redis
  mock_get_queue.return_value = mock_queue
  with api.app.test_request_context(content_type='application/json', json={'graph_feature': 'gender'}):
    assert api.post_job() == {'job_id': 'oohanid'}
  mock_get_redis.assert_any_call(RedisDb.JOB_DB)
  mock_redis.set.assert_called_once_with('oohanid', b'{"status":"queued","graph_feature":"gender"}')
  mock_redis.rpush.assert_called_once_with(REDIS_JOB_IDS_KEY, 'oohanid')
  mock_get_queue.assert_called_once_with()
  mock_queue.put.assert_called_once_with('oohanid')

@patch('api.get_queue')
@patch('api.get_redis')
@patch('api.uuid4')
def test_post_job_succeeds_on_valid_transactions_input(mock_uuid, mock_get_redis, mock_get_queue):
  mock_uuid.return_value = 'oohanid'
  mock_redis = Mock()
  mock_queue = Mock()
  mock_get_redis.return_value = mock_redis
  mock_get_queue.return_value = mock_queue
  with api.app.test_request_context(content_type='application/json', json={'transactions': [{
    'trans_date_trans_time': '21/06/2020 12:16',
    'merchant': 'amerchant',
    'category': 'acategory',
    'amt': 1.23,
    'lat': 4.56,
    'long': 7.89,
    'job': 'painter',
    'merch_lat': 7.0,
    'merch_long': 7.0,
  }]}):
    assert api.post_job() == {'job_id': 'oohanid'}
  mock_get_redis.assert_any_call(RedisDb.JOB_DB)
  mock_redis.set.assert_called_once_with('oohanid', b'{"status":"queued","transactions":[{"amt":1.23,"category":"acategory","job":"painter","lat":4.56,"long":7.89,"merch_lat":7.0,"merch_long":7.0,"merchant":"amerchant","trans_date_trans_time":"21/06/2020 12:16"}]}')
  mock_redis.rpush.assert_called_once_with(REDIS_JOB_IDS_KEY, 'oohanid')
  mock_get_queue.assert_called_once_with()
  mock_queue.put.assert_called_once_with('oohanid')

@patch('api.abort', side_effect=Exception)
@patch('api.get_redis')
def test_get_job_information_fails_on_invalid_jobid(mock_get_redis, mock_abort):
  mock_redis = Mock()
  mock_redis.get.return_value = None
  mock_get_redis.return_value = mock_redis
  with pytest.raises(Exception):
    api.get_job_information('anid')
  mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
  mock_redis.get.assert_called_once_with('anid')
  mock_abort.assert_called_once_with(400, 'Invalid job id.')

@patch('api.get_redis')
def test_get_job_information_succeeds_with_good_jobid(mock_get_redis):
  mock_redis = Mock()
  mock_redis.get.return_value = b'{"status":"queued","graph_feature":"gender"}'
  mock_get_redis.return_value = mock_redis
  assert api.get_job_information('anid') == {'status': 'queued', 'graph_feature': 'gender'}
  mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
  mock_redis.get.assert_called_once_with('anid')

@pytest.mark.parametrize('job_info,error_code,error_msg', [
  ({}, 500, 'Malformed job.'),
  ({'status': 8}, 400, 'Job is not complete. Current job status is 8.'),
  ({'status': 'queued'}, 400, 'Job is not complete. Current job status is queued.'),
])
def test_get_job_result_calls_abort_if_job_is_not_ready(job_info, error_code, error_msg):
  with patch('api.get_job_information') as mock_get_job_information:
    mock_get_job_information.return_value = job_info
    with patch('api.abort', side_effect=Exception) as mock_abort:
      with pytest.raises(Exception):
        api.get_job_result('ajobid')
      mock_abort.assert_called_once_with(error_code, error_msg)
    mock_get_job_information.assert_called_once_with('ajobid')

@patch('api.abort', side_effect=Exception)
@patch('api.get_redis')
@patch('api.get_job_information')
def test_get_job_calls_abort_if_no_result_is_found(mock_get_job_info, mock_get_redis, mock_abort):
  mock_get_job_info.return_value = {'status': 'completed'}
  mock_redis = Mock()
  mock_redis.get.return_value = None
  mock_get_redis.return_value = mock_redis
  with pytest.raises(Exception):
    api.get_job_result('ajobid')
  mock_get_job_info.assert_called_once_with('ajobid')
  mock_get_redis.assert_called_once_with(RedisDb.JOB_RESULTS_DB)
  mock_redis.get.assert_called_once_with('ajobid')
  mock_abort.assert_called_once_with(500, 'Job marked as completed but no result found in DB.')

@patch('api.send_file')
@patch('api.get_redis')
@patch('api.get_job_information')
def test_get_job_calls_send_file_if_a_result_is_found(mock_get_job_info, mock_get_redis, mock_send_file: Mock):
  mock_get_job_info.return_value = {'status': 'completed'}
  mock_redis = Mock()
  mock_redis.get.return_value = b'afakebinarystoredvalue'
  mock_get_redis.return_value = mock_redis
  mock_send_file.return_value = 'this totally unique result string'
  assert api.get_job_result('ajobid') == 'this totally unique result string'
  mock_get_job_info.assert_called_once_with('ajobid')
  mock_get_redis.assert_called_once_with(RedisDb.JOB_RESULTS_DB)
  mock_redis.get.assert_called_once_with('ajobid')
  assert len(mock_send_file.mock_calls) == 1
  send_file_call = mock_send_file.mock_calls[0]
  assert len(send_file_call.args) == 1
  assert len(send_file_call.kwargs) == 3
  assert send_file_call.args[0].getvalue() == b'afakebinarystoredvalue'
  assert send_file_call.kwargs == {'mimetype': 'image/png', 'as_attachment': True, 'download_name': 'plot ajobid.png'}

def test_get_help():
  assert isinstance(api.get_help(), str)