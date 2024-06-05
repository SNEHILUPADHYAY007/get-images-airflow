# Required Imports
import json
import pathlib
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Initializing the DAG
dag = DAG(                                   
    dag_id="download_rocket_launches",   
    start_date=airflow.utils.dates.days_ago(14),   
    schedule_interval=None,                     
)

# Function to get the images, download to docker path(COPY TO LOCAL PATH WILL BE DONE THROUGH MOUNTING)
def get_pictures():
    with open("/opt/airflow/tmp/launches.json") as f:
        res = json.load(f)
        # Simplifying json response by taking necessary info
        data_1 = [i["program"] for i in res["results"]]
        images_link = [j["image_url"] for i in data_1 for j in i]
        
        # Send request to download the images and save to local disk
        for img in images_link:
            """
            SCOPE:- ASYNC CALL TO GET THE IMAGES.
            """
            res = requests.get(img) 
            file_name = f"/opt/airflow/tmp/images/{img.split('/')[-1]}"
            with open(file_name,"wb") as f:
                f.write(res.content)

# To get the count of files at the path.
notify = BashOperator(
   task_id="notify",
   bash_command='echo "There are now $(ls /opt/airflow/tmp/images/ | wc -l) images."',
   dag=dag,
)

# Download the response from the API in json file and later use it to get the required info.
# Rate limited API, Hence following this to avoid more requests.
download_launches = BashOperator(    
    task_id="download_launches",                  
    bash_command="curl -o /opt/airflow/tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag,
)


# Operator to call get_pictures function.
get_pictures = PythonOperator(      
   task_id="get_pictures",
   python_callable=get_pictures,   
   dag=dag,
 )

# Task running sequence.
download_launches>>get_pictures>>notify

