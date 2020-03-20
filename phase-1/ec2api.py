import boto3 as bt
from botocore.exceptions import ClientError
import datetime

class AwsEC2Controller:
    def __init__(self, available_instances, timeout_interval=30, utilization_thresh=1, queue_name='ccp1queue.fifo'):
        print("Aws EC2 Controller Initialized")
        self.ec2 = bt.resource("ec2")
        self.ec2_client = bt.client("ec2")
        # self.count_all_instances = 19
        # saving one instance for scaling up
        # self.count_active = 0  # All instances are shutdown initially
        # self.count_inactive = 3  # ?

        self.sqs_client = bt.resource('sqs')
        self.message_queue = self.sqs_client.get_queue_by_name(QueueName=queue_name)
        # we need to maintain a list with available lists to spin up
        self.stopped_instances = available_instances
        # need a list of active instances to spin down
        self.active_instances = []
        # need a count of (# of messages coming in) / active
        self.request_count = 0
        self.start_interval_time = datetime.datetime.now()
        # self.avg_processing_time = avg_processing_time
        # this is in seconds
        self.timeout_interval = timeout_interval
        # self.utilization_thresh = utilization_thresh


    # def create_instances(self):
    #     pass

    # We will create instances manually and keep it in shutdown state, will save us time
    # --------------------------------#
    #           Start Instance        #
    # --------------------------------#
    def start_instance(self, instance_id):
        print("starting instance : ", instance_id)
        ec2_waiter = self.ec2_client.get_waiter("instance_running")
        try:
            #   Do a dryrun first to verify permissions
            self.ec2_client.start_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if "DryRunOperation" not in str(e):
                raise
        # Dry run succeeded, run start_instances without dryrun
        try:
            response = self.ec2_client.start_instances(
                InstanceIds=[instance_id], DryRun=False
            )
            ec2_waiter.wait(InstanceIds=[instance_id])
        except ClientError as e:
            print(e)

    # -------------------------#
    # start multiple instances #
    # -------------------------#
    def start_n_instances(self, instance_list):
        ec2_waiter = self.ec2_client.get_waiter("instance_running")
        responses = self.ec2_client.start_instances(
            InstanceIds=instance_list, DryRun=False
        )
        ec2_waiter.wait(InstanceIds=instance_list)

    def stop_n_instances(self, instance_list):
        ec2_waiter = self.ec2_client.get_waiter("instance_running")
        responses = self.ec2_client.stop_instances(
            InstanceIds=instance_list, DryRun=False
        )
        ec2_waiter.wait(InstanceIds=instance_list)
    def stop_instance(self, instance_id):
        print("Stopping instance", instance_id)
        ec2_waiter = self.ec2_client.get_waiter("instance_stopped")
        try:
            self.ec2_client.stop_instances(InstanceIds=[instance_id], DryRun=True)
        except ClientError as e:
            if "DryRunOperation" not in str(e):
                raise
        try:
            response = self.ec2_client.stop_instances(
                InstanceIds=[instance_id], DryRun=False
            )
        except ClientError as e:
            print(e)

    def count_instance(self, state="running"):
        pass

    def list_instance(self, state="running"):
        pass

    def detail_instance(self):
        pass

    # def parse_message(self, queue_url):
    #     response = sqs.receive_message(
    #         QueueUrl=queue_url,
    #         AttributeNames=[
    #             'SentTimestamp'
    #         ],
    #         MaxNumberOfMessages=1,
    #         MessageAttributeNames=[
    #             'All'
    #         ],
    #         VisibilityTimeout=0,
    #         WaitTimeSeconds=0
    #     )

        # if response is empty continue, otherwise add it to the worker queue.
        # if len(response['Messages']) > 0:
        #     if self.request_count/float(len(self.active_instances)) > self.utilization_thresh:
                # spin up more instances to handle the load
    def scale_instances(self):
        queue_count = self.message_queue.attributes['ApproximateNumberOfMessages']
        print('Queue message count: {}'.format(queue_count))
        if self.active_instances < queue_count:
            spinup_count = queue_count - self.active_instances
            spinup_nodes = self.stopped_instances[-spinup_count::]
            print('Spinning up: {}'.format(spinup_nodes))
            self.start_n_instances(spinup_nodes)
            self.stopped_instances = self.stopped_instances[:-spinup_count:]
            self.active_instances = self.active_instances + spinup_nodes

        diff_time = datetime.datetime.now() - self.start_interval_time
        if diff_time.seconds > self.timeout_interval:
            # check to see if we should spin down instances
            if queue_count > len(self.active_instances):
                spindown_count = queue_count - len(self.active_instances)
                spindown_nodes = self.active_instances[-spindown_count::]
                print('Spinning down: {}'.format(spindown_nodes))
                self.stop_n_instances(spindown_nodes)
                self.active_instances = self.active_instances[:-spindown_count:]
                self.stopped_instances = self.stopped_instances + spindown_nodes
            # reset timeout interval
            self.start_interval_time = datetime.datetime.now()

        print('Active Nodes: {}'.format(self.active_instances))
        print('Stopped Nodes: {}'.format(self.stopped_instances))


if __name__ == "__main__":
    instance_ids = []
    controller = AwsEC2Controller(instance_ids)
    # poll
    while True:
        controller.scale_instances()
    # obj.start_instance("i-07df79accca04995b")
    # obj.stop_instance("i-07df79accca04995b")

