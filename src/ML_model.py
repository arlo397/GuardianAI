import pandas as pd
import numpy as np
import seaborn as sns
import category_encoders as ce
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from sklearn.metrics import accuracy_score


def train_model() -> RandomForestClassifier:
    # Load data from a CSV file
    from api import get_data
    datos = pd.DataFrame(get_data())
    # Define the target variable and the features
    y = datos["is_fraud"]
    X = datos.drop("is_fraud", axis=1)

    # Split the data into training and testing sets
    X_train_val, X_test, y_train_val, y_test = train_test_split(X, y, test_size=0.30, random_state=123)
    X_train, X_val, y_train, y_val = train_test_split(X_train_val, y_train_val, test_size=0.30, random_state=123)

    # Drop unnecessary columns from the training, validation, and test sets
    X_train = X_train.drop(['cc_num', 'first', 'last', 'zip', 'trans_num'], axis=1)
    X_test = X_test.drop(['cc_num', 'first', 'last', 'zip', 'trans_num'], axis=1)
    X_val = X_val.drop(['cc_num', 'first', 'last', 'zip', 'trans_num'], axis=1)

    # Split date and time into separate columns
    X_train[['date', 'time']] = X_train['trans_date_trans_time'].str.split(' ', expand=True)
    X_test[['date', 'time']] = X_test['trans_date_trans_time'].str.split(' ', expand=True)
    X_val[['date', 'time']] = X_val['trans_date_trans_time'].str.split(' ', expand=True)

    # Further split the date into day, month, and year
    X_train[['day', 'month', 'year']] = X_train['date'].str.split('/', expand=True)
    X_test[['day', 'month', 'year']] = X_test['date'].str.split('/', expand=True)
    X_val[['day', 'month', 'year']] = X_val['date'].str.split('/', expand=True)

    # Process the 'time' column to extract the hour
    X_train['time'] = X_train['time'].apply(lambda x: x.split(':')[0])
    X_test['time'] = X_test['time'].apply(lambda x: x.split(':')[0])
    X_val['time'] = X_val['time'].apply(lambda x: x.split(':')[0])

    # Drop the original date and time columns and some other unused columns
    X_train = X_train.drop(['date', 'trans_date_trans_time', 'Unnamed: 0'], axis=1)
    X_test = X_test.drop(['date', 'trans_date_trans_time', 'Unnamed: 0'], axis=1)
    X_val = X_val.drop(['date', 'trans_date_trans_time', 'Unnamed: 0'], axis=1)

    # Process 'dob' to extract the year and convert gender to numeric
    X_train['dob'] = X_train['dob'].apply(lambda x: x.split('/')[-1])
    X_test['dob'] = X_test['dob'].apply(lambda x: x.split('/')[-1])
    X_val['dob'] = X_val['dob'].apply(lambda x: x.split('/')[-1])
    X_train["gender"] = X_train["gender"].replace({'M': 0, 'F': 1})
    X_test["gender"] = X_test["gender"].replace({'M': 0, 'F': 1})
    X_val["gender"] = X_val["gender"].replace({'M': 0, 'F': 1})

    # Binary encode categorical variables
    columnas_a_encodear = ["merchant", "category", "street", "job", "city", "state"]
    encoder = ce.BinaryEncoder(cols=columnas_a_encodear)
    train_encoded = encoder.fit_transform(X_train[columnas_a_encodear])
    val_encoded = encoder.transform(X_val[columnas_a_encodear])
    test_encoded = encoder.transform(X_test[columnas_a_encodear])

    # Drop the original categorical columns and join the encoded ones
    X_train = X_train.drop(columns=columnas_a_encodear).join(train_encoded)
    X_val = X_val.drop(columns=columnas_a_encodear).join(val_encoded)
    X_test = X_test.drop(columns=columnas_a_encodear).join(test_encoded)

    # Train the RandomForestClassifier
    model = RandomForestClassifier(random_state=321)
    model.fit(X_train, y_train)

    # Predict and evaluate the model on the validation set
    predictions = model.predict(X_val)
    accuracy = accuracy_score(y_val, predictions)
    print("Model accuracy:", accuracy)

    # Calculate feature importances and return them
    feature_importances = pd.DataFrame({
        'feature': X_train.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)

    return feature_importances
