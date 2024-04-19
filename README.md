## **GuardianAI**
### Team Members: Reem Fashho, Owen Scott, Yi Yang

#### Due Date: Friday, May 3, by 1:00 PM Central Time
##### https://coe-332-sp24.readthedocs.io/en/latest/homework/final.html

### Project Description
Our project focuses on leveraging the Credit Card Fraud Prediction Dataset available on Kaggle to create a robust containerized web application. This application will utilize databases for efficient data storage and management, enabling user querying and facilitating job queues. By employing Flask, we will develop API endpoints that provide users with access to comprehensive summary statistics and plots derived from the Credit Card Fraud dataset. More importantly, considering the widespread utilization of this dataset for machine learning-based fraud detection, we aim to design our application to accept credit card input for predicting potential fraud from a pre-trained model we develop. This will allow users to submit a job, and retrieve a prediction about whether the particular credit card attributes are likely fraudulent. 

### Project Importance
This project is essential as it tackles the pressing issue of credit card fraud by using advanced explainable AI techniques on the Credit Card Fraud Prediction Dataset from Kaggle. It will provide a containerized web application that offers real-time fraud prediction, enhancing security measures for financial institutions and protecting consumers from fraudulent transactions.

### Software Diagram
The following software diagram captures the primary components and workflow of our system... 

### Description of Folder Contents
- `Dockerfile`: Contains instructions for building a Docker image of our program
- `docker-compose.yaml`: YAML file used to replace running the `docker build` and `docker run` commands for deploying a container. Orchestrates 3 services together: Redis Database, Flask App, Worker.
- `src/api.py`: Main Python script that interacts with our fraud data set, hosts the Flask app that allows the user to query for information, as well as interacts with the Redis database. 
    - [GET] `/data`: Returns all data from Redis
    - [POST] `/data`: Puts data into Redis
    - [DELETE] `/data`: Deletes data in Redis
    - [GET] `/`: 
    - [GET] `/`: 
    - [GET] `/`:
    - [GET] `/jobs/<jobid>` : Returns all job information for a given JOB ID
    - [GET] `/jobs`: Returns all existing JOB IDs
    - [POST] `/jobs`: Creates a new job with a unique identifier (uuid)
        - The `/jobs` POST request must include a data packet in JSON format which is stored along with the job information. For our application, the client must provide the following JSON formatted data: 
          '{"Year Modified Start": <start_year>, "Year Modified End": <end_year>}' -H "Content-Type: application/json"
    - [GET] `/results/<jobid>`: Return requested job result in the form of a JSON dictionary. If the job has not yet been finished, the api returns a message indicating so.
- `src/jobs.py`: Initializes databases and provides the functionality to create/submit/put jobs on the queue. 
- `src/worker.py`: Pull jobs off of the queue and executes job functionality. 
- `requirements.txt`: Text file that lists all of the Python non-standard libraries used to develop the code.
- `data/`: Local Directory for Redis container to presist data to file system across container executions. 
- `test/test_api.py`: Tests functionality in `src/api.py`
- `test/test_jobs.py`: Tests functionailty in `src/jobs.py`
- `test/test_worker.py`: Tests functionailty in `src/worker.py`

Note: All throughout the code source, strategic logging is implemented to alert the developer of important events and bugs that arise. Logs are stored in `logger.log`

To view the logger for each container, execute the following:  `docker exec -it <container_id> bash`. `logger.log` is found in `/app`. Logs concerning the `worker` container are found by executing the command for the `worker` container id. Get the container id by running: `docker ps -a`. 

### Data 
##### Source: https://www.kaggle.com/datasets/kelvinkelue/credit-card-fraud-prediction
The dataset "Credit Card Fraud Prediction" is designed to evaluate and compare various fraud detection models. It comprises 555,719 records across 22 attributes, featuring a comprehensive mix of categorical and numerical data types with no missing values. Essential components of the dataset include:

- Transaction Details: Precise timestamps, merchant information, and transaction amounts.
- Fraud Indicator: A binary attribute marking transactions as fraudulent or legitimate, serving as the primary target for predictive modeling.
- Cardholder Information: Names, addresses, job titles, and demographics, providing a deep dive into the profiles involved in transactions.
- Geographical Data: Location details for both merchants and cardholders to explore spatial patterns in fraud occurrences.

This dataset is a rich resource that fosters the development, testing, and comparison of different fraud detection techniques. It is a valuable tool for researchers and practitioners dedicated to advancing the field of fraud detection through innovative modeling and analysis.

### Flask Application

### Instructions on How to Deploy Containerized Code with docker-compose
Once the code has been pulled, execute: `docker-compose up --build`. 
To have the container running in the background, add the `-d` tag to the command. 

This will effectively build and deploy the docker images as containers ensuring port to port mapping and proper organization and dependency between the containers. Specifically, the command builds the `worker` and `flask-app` images and pulls the `redis` stock image for use. 

Execute `docker ps -a` to ensure the three containers are up and runnning. You should see the following:
```
CONTAINER ID   IMAGE                           COMMAND                  CREATED          STATUS                      PORTS                                       NAMES
0ed6746af37c   username/fraud_detect_app:1.0   "python3 api.py"         58 seconds ago   Up 57 seconds               0.0.0.0:5000->5000/tcp, :::5000->5000/tcp   guardianai_flask-api_1
e89eb33406de   username/fraud_detect_app:1.0   "python3 worker.py"      58 seconds ago   Up 57 seconds                                               guardianai_worker_1
dc51e6ac5ae9   redis:7                         "docker-entrypoint.s…"   59 seconds ago   Up 58 seconds               0.0.0.0:6379->6379/tcp, :::6379->6379/tcp   guardianai_redis-db_1
```
* Note for developers: If you make edits to any of the contaner source files (i.e, `worker.py` or `app.py`), you can redeploy the containers by simply running: `docker-compose up --build -d <edited_image>` rather than executing `docker-copmpose down` followed by `docker-compose up --build -d` again. 

Once you have ensured that the microservice is up and running, you can access your application via curl and by specifying the port.

### Instructions For Accessing Web App Routes & Route Output Descriptions
While the service is up (after executing `docker-compose up`), you may curl the following example commands to interact with the application. 

#### Curl Commands to Routes: `curl http://<ipaddress>:port/route`
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

### Instructions on How to Run Test Cases
Unit tests for the application are stored in the `/tests` directory and copied over to the `app` directory in the container (alongside the main scripts). To execute the test scripts, enter into the respective container interactively and execute `pytest`.

Example: Run the following commands
```
docker exec -it <container_id> bash
ls # Check that the test scripts are in the `/app` directory

# Run pytest
pytest
```

### Instructions to Stop Microservice 
When you are ready to remove and kill the services, execute the following command: `docker-compose down`