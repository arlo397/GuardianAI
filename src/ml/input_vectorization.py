import csv
from datetime import datetime
from functools import partial
import logging
from multiprocessing import Pool, cpu_count
import random
from typing import Any, Optional

# Columns are all strings in the following order
# Transaction date and time (dd/mm/YYYY HH:MM)
# CC Number
# Name of Merchant
# Category of transaction/commerce
# Transaction amount
# First name
# Last name
# Gender
# Street
# City
# State
# Zip
# Transaction latitude (deg)
# Transaction longitude (deg)
# City population
# Job description
# Date of birth
# Transaction number (meaningless hash)
# Unix timestamp
# Merchant latitude (deg)
# Merchant longitude (deg)
# Is Fraud (0 = no, 1 = yes)

INPUT_SIZE = 1196 # This was calculated by code len(training_inputs[0]) since we onehot encode things and flatten that input vector
# If the input format changes go back to using len and then update this hardcoded value

def extract_row(dict: dict[str, str]) -> tuple[list[Any], int]:
  date = datetime.strptime(dict['trans_date_trans_time'], '%d/%m/%Y %H:%M')
  result = [date.day, date.month, date.year, date.weekday(), date.hour, date.minute, dict['merchant'], dict['category'], float(dict['amt']), float(dict['lat']), float(dict['long']), dict['job'], float(dict['merch_lat']), float(dict['merch_long']), int(dict['is_fraud'])]
  return result

def _onehot_encode(row: list[Any], merchants: list[str], categories: list[str], jobs: list[str]) -> list[Any]:
  row[6] = [1 if row[6] == m else 0 for m in merchants]
  row[7] = [1 if row[7] == c else 0 for c in categories]
  row[11] = [1 if row[11] == j else 0 for j in jobs]
  return [item if isinstance(item, list) else item for item in row]

def _flatten(lst: list[Any]) -> list[Any]:
  flattened = []
  for item in lst:
    if isinstance(item, list):
      flattened.extend(_flatten(item))
    else:
      flattened.append(item)
  return flattened

class TestValidateTrainSplit():
  def __init__(self, filename: str, train_pct: float = 0.75, validation_pct: float = 0.15, test_pct: float = 0.1):
    # Valid inputs
    assert train_pct > 0
    assert validation_pct >= 0
    assert test_pct > 0
    assert round((train_pct + validation_pct + test_pct) * 100) == 100
    self.train_pct = train_pct
    self.validation_pct = validation_pct
    self.test_pct = test_pct
    merchants = set()
    categories = set()
    jobs = set()
    # Read in the important data
    logging.info('Reading data...')
    with open(filename, 'r') as data:
      reader = csv.DictReader(data)
      self.rows = [extract_row(row) for row in reader] # Randomize order for statistically good test/train/validation split later
    logging.info('Reading categorical data')
    # One-hot encode our word-based inputs
    for row in self.rows:
      merchants.add(row[6])
      categories.add(row[7])
      jobs.add(row[11])
    merchants = sorted(merchants)
    categories = sorted(categories)
    jobs = sorted(jobs)
    encoder = partial(_onehot_encode, merchants=merchants, categories=categories, jobs=jobs)
    logging.info('Transforming data')
    with Pool(processes=cpu_count()) as pool:
      self.rows = pool.map(encoder, self.rows)
      self.rows = pool.map(_flatten, self.rows)
    logging.info('Shuffling data')
    random.shuffle(self.rows)
    logging.info('Done preparing data')

  def get_train_inputs(self) -> list[Any]:
    train_end = round(self.train_pct * len(self.rows))
    return [row[:-1] for row in self.rows[0:train_end]]

  def get_train_labels(self) -> list[int]:
    train_end = round(self.train_pct * len(self.rows))
    return [row[-1] for row in self.rows[0:train_end]]
  def get_validation_inputs(self) -> Optional[list[Any]]:
    if self.validation_pct == 0: return None
    validation_start = round(self.train_pct * len(self.rows))
    validation_end = round((self.train_pct + self.validation_pct) * len(self.rows))
    return [row[:-1] for row in self.rows[validation_start:validation_end]]

  def get_validation_labels(self) -> Optional[list[int]]:
    if self.validation_pct == 0: return None
    validation_start = round(self.train_pct * len(self.rows))
    validation_end = round((self.train_pct + self.validation_pct) * len(self.rows))
    return [row[-1] for row in self.rows[validation_start:validation_end]]

  def get_test_inputs(self) -> list[Any]:
    test_start = round((self.train_pct + self.validation_pct) * len(self.rows))
    return [row[:-1] for row in self.rows[test_start:]]

  def get_test_labels(self) -> list[int]:
    test_start = round((self.train_pct + self.validation_pct) * len(self.rows))
    return [row[-1] for row in self.rows[test_start:]]
