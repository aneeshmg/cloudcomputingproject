import boto3
import json
import random
import os
import time
from botocore.exceptions import ClientError
import subprocess

video_repo_directory = '/home/ubuntu/videos/'
sqs_client = boto3.resource('sqs')
queue = sqs_client.get_queue_by_name(QueueName='ccp1queue.fifo')

def download_s3(bucket, object_name, local_name):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).download_file(object_name, local_name)

def upload_to_s3(result, bucket, object_name):
    s3_client = boto3.client('s3')

    with open(video_repo_directory + object_name + '.txt', 'w+') as f:
        f.write(result)

    response = s3_client.upload_file(video_repo_directory + object_name + '.txt', bucket, object_name)
    print(response)
    return True

def generate_results(results_file):
    with open(results_file) as f:
        res = f.readlines()

    results_map = {}

    for line in res:
        if ':' in line and '%' in line:
            line = line.strip()
            obj, confidence = line.split(':')
            obj, confidence = obj.strip(), confidence.strip()
            confidence = int(confidence.split('%')[0])
            if obj in results_map:
                old_confidence = results_map[obj]
                confidence = max(confidence, old_confidence)
                results_map[obj] = confidence
            else:
                results_map[obj] = confidence

    return str(results_map)

def get_result(video):
    result_file = video_repo_directory + video + '.txt'
    os.system("bash /home/ubuntu/cloudcomputingproject/phase-1/run-darknet.sh "+ video)

    result = generate_results(result_file)

    return result

def clean_up(video):
    os.remove(video_repo_directory + video)
    os.remove(video_repo_directory + video + '.txt')
    return True


while True:
    for message in queue.receive_messages():
        # Print out the body of the message
        print(message.body)
        video_name = message.body

        download_s3('ccp1inputs', video_name, video_repo_directory + video_name)

        result = get_result(video_name)

        upload_to_s3(result, 'ccp1outputs', video_name)

        # Let the queue know that the message is processed
        message.delete()
        clean_up(video_name)
        print("Pending in queue" + queue.attributes['ApproximateNumberOfMessages'])