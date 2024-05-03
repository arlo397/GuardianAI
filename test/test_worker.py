import pytest
from unittest.mock import patch, Mock
from services import RedisDb
import worker

@patch('worker.get_redis')
def test_begin_job_throws_on_invalid_job_id(mock_get_redis):
  mock_redis = Mock()
  mock_redis.get.return_value = None
  mock_get_redis.return_value = mock_redis
  with pytest.raises(Exception, match='Job not found in Redis.'):
    worker._begin_job('a fake job id')
  mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
  mock_redis.get.assert_called_once_with('a fake job id')

@patch('worker.get_redis')
def test_begin_job_marks_valid_job_as_in_progress(mock_get_redis):
  mock_redis = Mock()
  mock_redis.get.return_value = b'{"some job info":"info","status":"queued"}'
  mock_get_redis.return_value = mock_redis
  assert worker._begin_job('job id') == {
    'some job info': 'info',
    'status': 'in_progress'
  }
  mock_get_redis.assert_any_call(RedisDb.JOB_DB)
  assert mock_get_redis.call_count == 2
  mock_redis.get.assert_called_once_with('job id')
  mock_redis.set.assert_called_once_with('job id', b'{"some job info":"info","status":"in_progress"}')

def test_extract_row():
  assert worker._extract_row({
    'trans_date_trans_time': '01/02/2024 12:34',
    'merchant': 'a merchant',
    'category': 'a category',
    'amt': 7.44,
    'lat': 0.12,
    'long': 3.45,
    'job': 'driveway vacuumer',
    'merch_lat': 6.78,
    'merch_long': 9.01,
  }) == [1, 2, 2024, 3, 12, 34, 'a merchant', 'a category', 7.44, 0.12, 3.45, 'driveway vacuumer', 6.78, 9.01]

# TODO: INSERT _execute_job TESTS HERE

@patch('worker._execute_graph_feature_analysis_job')
def test_execute_job_works_for_graph_feature(mock_execute_graph_feature_analysis_job):
  mock_execute_graph_feature_analysis_job.return_value = 7
  assert worker._execute_job('anid', {'graph_feature': 'whatever'}) == 7
  mock_execute_graph_feature_analysis_job.assert_called_once_with('anid', {'graph_feature': 'whatever'})

@patch('worker._execute_transaction_analysis_job')
def test_execute_job_works_for_transaction(mock_execute_transaction_analysis_job):
  mock_execute_transaction_analysis_job.return_value = 8
  assert worker._execute_job('ajobid', {'transactions': []}) == 8
  mock_execute_transaction_analysis_job.assert_called_once_with('ajobid', {'transactions': []})

def test_execute_returns_false_for_invalid_job():
  assert not worker._execute_job('ajobid', {'notarealkey': 'uhoh'})

@pytest.mark.parametrize('success,expected_status', [
  (True, 'completed'),
  (False, 'failed'),
])
def test_complete_job(success: bool, expected_status: str):
  with patch('worker.get_redis') as mock_get_redis:
    mock_redis = Mock()
    mock_get_redis.return_value = mock_redis
    worker._complete_job('a job id', {'status': 'in_progress'}, success)
    mock_get_redis.assert_called_once_with(RedisDb.JOB_DB)
    expected_bytes = ('{"status": "' + expected_status + '"}').encode()
    mock_redis.set.asset_called_once_with('a job id', expected_bytes)
