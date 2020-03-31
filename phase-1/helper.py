import time
import boto3 as bt
import datetime

class Tester:
    def __init__(self, queue_name='ccp1queue.fifo'):
        self.ec2 = bt.resource("ec2")
        self.ec2_client = bt.client("ec2")
        self.start_time = datetime.datetime.now()
        self.end_time = datetime.datetime.now()
        self.sqs_client = bt.resource('sqs')
        self.queue_name = queue_name

    def monitor(self):
        hasBegun = False
        while True:
            message_queue = self.sqs_client.get_queue_by_name(QueueName=self.queue_name)
            queue_count = int(message_queue.attributes['ApproximateNumberOfMessages'])
            print('Queue message count: {}'.format(queue_count))
            active_ids = self.parse_ids(self.get_filtered_instances(['running']))
            inactive_ids = self.parse_ids(self.get_filtered_instances(['stopped']))
            print('Running -> {}, {}'.format(active_ids, len(active_ids)))
            print('Stopped -> {}, {}'.format(inactive_ids, len(inactive_ids)))
            if queue_count > 0 and not hasBegun:
                hasBegun = True
                self.start_time = datetime.datetime.now()
            elif queue_count == 0 and hasBegun and len(active_ids) == 1:
                self.end_time = datetime.datetime.now()
                return self.start_time, self.end_time
            if hasBegun:
                time.sleep(1)

    def get_filtered_instances(self, states):
        return self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': states}])

    def parse_ids(self, collection):
        ids = []
        for instance in collection:
            ids.append(instance.id)
        return ids


if __name__ == '__main__':
    tester = Tester()
    start, end = tester.monitor()
    diff = end - start
    print('Start: {}, End: {}'.format(start, end))
    print(diff.seconds)
