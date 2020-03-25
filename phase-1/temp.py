import boto3
import json
import random
import os
import time
from botocore.exceptions import ClientError

def upload_to_s3(file_name, bucket, object_name):
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
    print(response)
    return True

def download_s3(bucket, object_name, local_name):
    s3 = boto3.resource('s3')
    s3.Bucket(bucket).download_file(object_name, local_name)



sqs_client = boto3.resource('sqs')
queue = sqs_client.get_queue_by_name(QueueName='ccp1queue.fifo')

def put_messages(n):
    for i in range(n):
        video_name = 'video' + str(i) + '.h264'
        print('Uploading ' + video_name)
        response = queue.send_message(
            MessageBody=video_name,
            MessageGroupId='messageGroup1'
        )
        # upload_to_s3('./video.h264', 'ccp1inputs', video_name)

        print(response)

put_messages(10)

time.sleep(2)

print("\n\n\n")
print(queue.attributes['ApproximateNumberOfMessages'])

# while True:
#     for message in queue.receive_messages():
#         print(message.body)

#         time.sleep(2)

#         print(queue.attributes['ApproximateNumberOfMessages'])

#         # Let the queue know that the message is processed
#         message.delete()


# darknet command - ./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights video.h264
# os.system('ls')




# upload_to_s3('./video.h264', 'ccp1inputs', 'sample-video')

# download_s3('ccp1inputs', 'sample-video', 'test.h264')