import boto3
import json
import os
import time


video_repo_directory = "/home/pi/video/"
temp_video_directory = "/home/pi/temp_videos/"

sqs_client = boto3.resource('sqs')
queue = sqs_client.get_queue_by_name(QueueName='ccp1queue.fifo')


def upload_result_to_s3(result, bucket, object_name):
    s3_client = boto3.client('s3')

    with open(video_repo_directory + object_name + '.txt', 'w+') as f:
        f.write(result)

    response = s3_client.upload_file(video_repo_directory + object_name + '.txt', bucket, object_name)
    print(response)
    return True

def upload_video_to_s3(file_name, bucket, object_name):
    s3_client = boto3.client('s3')
    response = s3_client.upload_file(file_name, bucket, object_name)
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
    result_file = temp_video_directory + video + '.txt'
    os.system("bash /home/pi/cloudcomputingproject/phase-1/run-darknet-pi.sh "+ video)

    result = generate_results(result_file)

    return result

def clean_up(video):
    os.remove(temp_video_directory + video)
    os.remove(temp_video_directory + video + '.txt')
    return True

i = 0
while True:
    # get video from recorded dir
    # move video into temp dir
    # run darknet
    # upload results
    # cleanup
    print("Pi Worker started...")
    videos_list = sorted(os.listdir(video_repo_directory))
    time.sleep(1)
    if len(videos_list) > 0:
        video_name = videos_list[0]
        print("About to process " + video_name)

        os.rename(video_repo_directory + video_name, temp_video_directory + video_name)

        result = get_result(video_name)

        upload_result_to_s3(result, 'ccp1outputs', video_name)
        upload_video_to_s3(temp_video_directory + video_name, 'ccp1inputs', video_name)

        clean_up(video_name)
