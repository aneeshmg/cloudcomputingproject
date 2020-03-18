import boto3 as bt
from botocore.exceptions import ClientError
import time
import json


class AwsSQSController:
    def __init__(self, debug=False):

        self.debug = debug

        print("Initialized SQS Controller")
        self.sqs_client = bt.client("sqs")
        self.sqs = bt.resource("sqs")
        self.sqs_queue_names = ["data-info", "task-assignment", "idle-vm"]
        self.prefix = "cloud-project1-team-any-"

    def create_queue(self, queue_name="all", prefix=""):
        pass

    def delete_queue(self, queue_name="all", prefix=""):
        pass

    def send_msg(self, queue_name, msg, value):
        pass

    def receive_msg(self, queue_name, msg, max_msgs=1):
        pass

    def check_queue_size(self, queue_name):
        pass

    def output_msg(self, queue_name, msg, max_msgs=1):
        pass

    def read_message(self, response, q_url):
        pass


if __name__ == "__main__":
    obj = AwsSQSController(debug=True)
