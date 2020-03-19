import boto3
import json
import random
import os
import time
from botocore.exceptions import ClientError

video_repo_directory = '/home/ubuntu/videos'

def download_s3(bucket, object_name, local_name):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).download_file(object_name, local_name)

def upload_to_s3(result, bucket, object_name):
    s3_client = boto3.client('s3')

    with open('./temp/' + object_name, 'w+') as f:
        f.write(result)

    response = s3_client.upload_file('./temp/' + object_name, bucket, object_name)
    print(response)
    return True


def get_result(video):
    return video + " dog:5%"

sqs_client = boto3.resource('sqs')
queue = sqs_client.get_queue_by_name(QueueName='ccp1queue.fifo')

print(queue.attributes['ApproximateNumberOfMessages'])

while True:
    for message in queue.receive_messages():
        # Print out the body of the message
        print(message.body)

        download_s3('ccp1inputs', message.body, video_repo_directory + message.body)

        result = get_result(message.body)

        upload_to_s3(result, 'ccp1outputs', message.body)

        # Let the queue know that the message is processed
        message.delete()
        time.sleep(1)
        print(queue.attributes['ApproximateNumberOfMessages'])