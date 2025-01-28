import json
import csv
import os
from google.cloud import pubsub_v1
import glob     

files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

project_id = "trusty-lock-449117-h8";
topic_id = "labelsDesign"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def read_csv_and_publish(csv_file):
    with open(csv_file, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            message = json.dumps(row).encode("utf-8")
            future = publisher.publish(topic_path, message)
            print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    csv_file = "Labels.csv"
    read_csv_and_publish(csv_file)
