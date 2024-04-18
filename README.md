## **GuardianAI**
### Team Members: Reem Fashho, Owen Scott, Yi Yang

#### Due Date: Friday, May 3, by 1:00 PM Central Time
##### https://coe-332-sp24.readthedocs.io/en/latest/homework/final.html

### Project Description
Our project focuses on leveraging the Credit Card Fraud Prediction Dataset available on Kaggle to create a robust containerized web application. This application will utilize databases for efficient data storage and management, enabling user querying and facilitating job queues. By employing Flask, we will develop API endpoints that provide users with access to comprehensive summary statistics and plots derived from the Credit Card Fraud dataset. More importantly, considering the widespread utilization of this dataset for machine learning-based fraud detection, we aim to design our application to accept credit card input for predicting potential fraud from a pre-trained model we develop. This will allow users to submit a job, and retrieve a prediction about whether the particular credit card attributes are likely fraudulent. 

### Project Importance
This project is essential as it tackles the pressing issue of credit card fraud by using advanced explainable AI techniques on the Credit Card Fraud Prediction Dataset from Kaggle. It will provide a containerized web application that offers real-time fraud prediction, enhancing security measures for financial institutions and protecting consumers from fraudulent transactions.

### Software Diagram
The following software diagram captures the primary components and workflow of our system, describing the process in which... 

### Data 
##### Source: https://www.kaggle.com/datasets/kelvinkelue/credit-card-fraud-prediction
The dataset "Credit Card Fraud Prediction" is designed to evaluate and compare various fraud detection models. It comprises 555,719 records across 22 attributes, featuring a comprehensive mix of categorical and numerical data types with no missing values. Essential components of the dataset include:

- Transaction Details: Precise timestamps, merchant information, and transaction amounts.
- Fraud Indicator: A binary attribute marking transactions as fraudulent or legitimate, serving as the primary target for predictive modeling.
- Cardholder Information: Names, addresses, job titles, and demographics, providing a deep dive into the profiles involved in transactions.
- Geographical Data: Location details for both merchants and cardholders to explore spatial patterns in fraud occurrences.

This dataset is a rich resource that fosters the development, testing, and comparison of different fraud detection techniques. It is a valuable tool for researchers and practitioners dedicated to advancing the field of fraud detection through innovative modeling and analysis.

### Description of Folder Contents


### Flask Application

### Diagram

### Instructions on How to Deploy Containerized Code with docker-compose

### Instructions on How to Run Test Cases

1. **Data Example Endpoint**

   - **Description**: This endpoint provides a quick look at the dataset by returning the first five entries.

     ```shell
     curl http://localhost:5000/data_example
     ```

   - *expected output*

     ```shell
     [
       {
         "Unnamed: 0": 0,
         "amt": 2.86,
         "category": "personal_care",
         "cc_num": 2291160000000000.0,
         "city": "Columbia",
         "city_pop": 333497,
         "dob": "19/03/1968",
         "first": "Jeff",
         "gender": "M",
         "is_fraud": 0,
         "job": "Mechanical engineer",
         "last": "Elliott",
         "lat": 33.9659,
         "long": -80.9355,
         "merch_lat": 33.986391,
         "merch_long": -81.200714,
         "merchant": "fraud_Kirlin and Sons",
         "state": "SC",
         "street": "351 Darlene Green",
         "trans_date_trans_time": "21/06/2020 12:14",
         "trans_num": "2da90c7d74bd46a0caf3777415b3ebd3",
         "unix_time": 1371816865,
         "zip": 29209
       },
     ...
     ```

2. **Amount Analysis Endpoint**

   - **Description**: This endpoint provides statistical summaries of the transaction amounts.

     ```shell
     curl http://localhost:5000/amt_analysis
     ```

   - *expected output*

     - **count**: Total number of transactions.
     - **mean**: Average amount of transactions.
     - **std**: Standard deviation of the transaction amounts.
     - **min**: Minimum transaction amount.
     - **25%**: 25th percentile of the transaction amounts.
     - **50%** (median): Median of the transaction amounts.
     - **75%**: 75th percentile of the transaction amounts.
     - **max**: Maximum transaction amount.

     ```shell
     {
       "25%": 9.63,
       "50%": 47.29,
       "75%": 83.01,
       "count": 555719.0,
       "max": 22768.11,
       "mean": 69.39281023322938,
       "min": 1.0,
       "std": 156.74594135531336
     }
     ```

3. **Amount-Fraud Correlation Endpoint**

   - **Description**: This endpoint calculates the correlation between transaction amounts (`amt`) and their fraud status (`is_fraud`). Correlation measures the degree to which two variables move in relation to each other. A higher positive correlation means that higher transaction amounts might be more associated with fraudulent transactions, whereas a negative correlation would indicate the opposite.

     ```shell
     curl http://localhost:5000/amt_fraud_correlation
     ```

   - *expected output*

     ```shell
     {
       "amt": {
         "amt": 1.0,
         "is_fraud": 0.18226707130820347
       },
       "is_fraud": {
         "amt": 0.18226707130820347,
         "is_fraud": 1.0
       }
     }
     ```

4. **Fraudulent Zipcode Information Endpoint**

   - **Description**: This endpoint calculates which zipcode has the highest number of fraudulent transactions from the `fraud_test.csv` dataset and retrieves geographical information for that zipcode. It serves to identify potential hotspots of fraudulent activity and provides a quick link to view the location on Google Maps.

     ```shell
     curl http://localhost:5000/fraudulent_zipcode_info
     ```

   - *expected output*

     - **most_fraudulent_zipcode**: The zipcode with the highest number of fraud cases.
     - **fraud_count**: The number of frauds recorded in that zipcode.
     - **latitude** and **longitude**: Geographic coordinates of the zipcode.
     - **Google Maps Link**: Direct link to view the location on Google Maps.

     ```shell
     {
       "Google Maps Link": "https://www.google.com/maps/search/?api=1&query=38.02014542,-97.67005157",    
       "fraud_count": 19,
       "latitude": 38.02014542,
       "longitude": -97.67005157,
       "most_fraudulent_zipcode": "67020"
     }
     ```

   ![Alt text](googlemap_location.jpg)
