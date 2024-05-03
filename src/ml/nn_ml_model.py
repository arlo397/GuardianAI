import logging
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.utils.data import DataLoader, TensorDataset
from torcheval.metrics import BinaryAUROC

from input_vectorization import TestValidateTrainSplit, INPUT_SIZE

class CreditCardFraudDetectionModel(nn.Module):
  def __init__(self):
    super(CreditCardFraudDetectionModel, self).__init__()
    self.fc1 = nn.Linear(INPUT_SIZE, 64)
    self.fc2 = nn.Linear(64, 32)
    self.fc3 = nn.Linear(32, 1)
    self.bn1 = nn.BatchNorm1d(64)
    self.bn2 = nn.BatchNorm1d(32)
    self.relu = nn.ReLU()
    self.sigmoid = nn.Sigmoid()

  def forward(self, x):
    x = self.relu(self.bn1(self.fc1(x)))
    x = self.relu(self.bn2(self.fc2(x)))
    x = self.sigmoid(self.fc3(x))
    return x

def standardize_tensor(tensor: torch.Tensor) -> torch.Tensor:
  mean = torch.mean(tensor, dim=0)
  std = torch.std(tensor, dim=0)
  return (tensor - mean) / (std + 1e-8)


def train_and_save(datasource: TestValidateTrainSplit, save_path: str) -> CreditCardFraudDetectionModel:
  model = CreditCardFraudDetectionModel()
  loss_func = nn.BCELoss()
  optimizer = Adam(model.parameters(), lr=0.001)
  logging.info('Model, loss function, optimizer initialized')

  training_inputs = standardize_tensor(torch.tensor(datasource.get_train_inputs(), dtype=torch.float))
  training_labels = torch.tensor(datasource.get_train_labels(), dtype=torch.float)
  logging.info('Training tensors generated')
  training_dataset = TensorDataset(training_inputs, training_labels)
  training_data_loader = DataLoader(training_dataset, batch_size=64, shuffle=True)

  epochs = 45
  for epoch in range(epochs):
    model.train()
    logging.info(f'Begin epoch {epoch + 1}')
    for inputs, labels in training_data_loader:
      optimizer.zero_grad()
      outputs = model(inputs)
      loss = loss_func(outputs.squeeze(), labels)
      loss.backward()
      optimizer.step()
    logging.info(f'Epoch [{epoch + 1}/{epochs}], Loss: {loss.item()}')
  torch.save(model.state_dict(), save_path)
  return model

def load_saved_model(save_path: str) -> CreditCardFraudDetectionModel:
  model = CreditCardFraudDetectionModel()
  model.load_state_dict(torch.load(save_path))
  return model

def evaluate_model(model: CreditCardFraudDetectionModel, datasource: TestValidateTrainSplit):
  test_inputs = standardize_tensor(torch.tensor(datasource.get_test_inputs(), dtype=torch.float))
  test_labels = torch.tensor(datasource.get_test_labels(), dtype=torch.float)
  logging.info('Testing tensors generated')
  model.eval()
  with torch.no_grad():
    logging.info('Begin testing...')
    predictions = model(test_inputs).round().squeeze()
    true_positives = 0
    false_positives = 0
    true_negatives = 0
    false_negatives = 0
    for i in range(len(predictions)):
      if predictions[i] == 1 and test_labels[i] == 1: true_positives += 1
      elif predictions[i] == 1 and test_labels[i] == 0: false_positives += 1
      elif predictions[i] == 0 and test_labels[i] == 1: false_negatives += 1
      else: true_negatives += 1
    accuracy = (true_positives + true_negatives) / (true_negatives + true_positives + false_positives + false_negatives)
    precision = true_positives / (true_positives + false_positives) # specificity
    recall = true_positives / (true_positives + false_negatives) # sensitivity
    f1 = (2 * precision * recall) / (precision + recall)
    print(f'True positives: {true_positives}')
    print(f'True negatives: {true_negatives}')
    print(f'False positives: {false_positives}')
    print(f'False negatives: {false_negatives}')
    print(f'Accuracy: {accuracy}')
    print(f'Precision: {precision}')
    print(f'Recall: {recall}')
    print(f'F1: {f1}')
    metric = BinaryAUROC()
    metric.update(predictions, test_labels)
    print(f'AUROC: {metric.compute().item()}')

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  datasource = TestValidateTrainSplit('data-fallback/fraud_test.csv')
  model = train_and_save(datasource, 'binaryclassifierstate.pt')
  # model = load_saved_model('binaryclassifierstate.pt')
  evaluate_model(model, datasource)