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

# TODO: INSET _execute_job TESTS HERE

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
