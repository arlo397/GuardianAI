import logging
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.utils.data import DataLoader, TensorDataset
from input_vectorization import TestValidateTrainSplit

class CreditCardFraudDetectionModel(nn.Module):
  def __init__(self, input_size):
    super(CreditCardFraudDetectionModel, self).__init__()
    self.fc1 = nn.Linear(input_size, 64)
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


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  datasource = TestValidateTrainSplit('ml/fraud test.csv')
  training_inputs = standardize_tensor(torch.tensor(datasource.get_train_inputs(), dtype=torch.float))
  training_labels = torch.tensor(datasource.get_train_labels(), dtype=torch.float)
  logging.info('Training tensors generated')
  test_inputs = standardize_tensor(torch.tensor(datasource.get_test_inputs(), dtype=torch.float))
  test_labels = torch.tensor(datasource.get_test_labels(), dtype=torch.float)
  logging.info('Testing tensors generated')
  model = CreditCardFraudDetectionModel(len(test_inputs[0]))
  loss_func = nn.BCELoss()
  optimizer = Adam(model.parameters(), lr=0.001)
  logging.info('Model, loss function, optimizer initialized')

  training_dataset = TensorDataset(training_inputs, training_labels)
  training_data_loader = DataLoader(training_dataset, batch_size=64, shuffle=True)

  epochs = 60
  for epoch in range(epochs):
    model.train()
    logging.info(f'Begin epoch {epoch + 1}')
    for inputs, labels in training_data_loader:
      optimizer.zero_grad()
      outputs = model(inputs)
      loss = loss_func(outputs.squeeze(), labels)
      loss.backward()
      optimizer.step()
    logging.info(f'Epoch [{epoch + 1}/{60}], Loss: {loss.item()}')

  model.eval()
  with torch.no_grad():
    logging.info('Begin testing...')
    predictions = model(test_inputs)
    accuracy = (predictions.round() == test_labels).float().mean()
    logging.info(f'Accuracy: {accuracy.item()}')