import boto3 as bt
from botocore.exceptions import ClientError
import datetime
import time

class AwsEC2Controller:
    def __init__(self, timeout_interval=30, queue_name='ccp1queue.fifo'):
        print("Aws EC2 Controller Initialized")
        self.ec2 = bt.resource("ec2")
        self.ec2_client = bt.client("ec2")
        # self.count_all_instances = 19
        # saving one instance for scaling up
        # self.count_active = 0  # All instances are shutdown initially
        # self.count_inactive = 3  # ?
        self.queue_name = queue_name
        self.sqs_client = bt.resource('sqs')
        # self.message_queue = self.sqs_client.get_queue_by_name(QueueName=queue_name)
        instance_ids = self.parse_ids(self.get_filtered_instances(['stopped']))
        # for instance in self.get_filtered_instances(['stopped']):
        #     instance_ids.append(instance.id)
        print(instance_ids)
        print(len(instance_ids))
        #testing
        for instance in self.get_filtered_instances(['running']):
            print('running: {}'.format(instance.id))
        # we need to maintain a list with available lists to spin up
        self.stopped_instances = instance_ids[::]
        # need a list of active instances to spin down
        self.active_instances = []
        self.start_interval_time = datetime.datetime.now()
        # self.avg_processing_time = avg_processing_time
        # this is in seconds
        self.timeout_interval = timeout_interval

        # this is for the the circular queue implementation
        self.available_instances = instance_ids[::]
        self.instance_idx = 0
        self.seen = set()
        self.nodes_timeout = {}


    def parse_ids(self, collection):
        ids = []
        for instance in collection:
            ids.append(instance.id)
        return ids
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
        # ec2_waiter = self.ec2_client.get_waiter("instance_running")
        # responses = self.ec2_client.start_instances(
        #     InstanceIds=instance_list, DryRun=False
        # )
        # for i in instance_list:
        #     self.nodes_timeout[i] = datetime.datetime.now()
        # ec2_waiter.wait(InstanceIds=instance_list)
        for i in instance_list:
            responses = self.ec2_client.start_instances(
                InstanceIds=[i], DryRun=False
            )
            time.sleep(5)
        print('stopped -> running: {}'.format(instance_list))

    def stop_n_instances(self, instance_list):
        ec2_waiter = self.ec2_client.get_waiter("instance_stopped")
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

    # def scale_instances_timeout(self):
    #     message_queue = self.sqs_client.get_queue_by_name(QueueName=self.queue_name)
    #     queue_count = int(message_queue.attributes['ApproximateNumberOfMessages'])
    #     print('Queue message count: {}'.format(queue_count))
    #     total_instances = len(self.active_instances) + len(self.stopped_instances)
    #     # if we have more items in the queue than nodes, spin more up
    #     if len(self.active_instances) < queue_count and len(self.active_instances) != total_instances:
    #         spinup_count = queue_count - len(self.active_instances)
    #         spinup_nodes = self.stopped_instances[-spinup_count::]
    #         print('Spinning up: {}'.format(spinup_nodes))
    #         self.start_n_instances(spinup_nodes)
    #         self.stopped_instances = self.stopped_instances[:-spinup_count:]
    #         self.active_instances = self.active_instances + spinup_nodes
    #
    #     diff_time = datetime.datetime.now() - self.start_interval_time
    #     print('diff time: {}'.format(diff_time))
    #     # only check to spindown nodes after a certain time interval
    #     if diff_time.seconds > self.timeout_interval:
    #         # check to see if we should spin down instances
    #         if queue_count < len(self.active_instances):
    #             spindown_count = len(self.active_instances) - queue_count
    #             spindown_nodes = self.active_instances[-spindown_count::]
    #             print('Spinning down: {}'.format(spindown_nodes))
    #             self.stop_n_instances(spindown_nodes)
    #             self.active_instances = self.active_instances[:-spindown_count:]
    #             self.stopped_instances = self.stopped_instances + spindown_nodes
    #         # reset timeout interval
    #         self.start_interval_time = datetime.datetime.now()
    #
    #     print('Active Nodes: {}'.format(self.active_instances))
    #     print('Stopped Nodes: {}'.format(self.stopped_instances))

    # this scaling implementation will assume that the nodes close after completing their tasks
    # def scale_instance_circular(self):
    #     message_queue = self.sqs_client.get_queue_by_name(QueueName=self.queue_name)
    #     queue_count = int(message_queue.attributes['ApproximateNumberOfMessages'])
    #     print('Queue message count: {}'.format(queue_count))
    #     diff_time = datetime.datetime.now() - self.start_interval_time
    #     # only directly poll the queue if the timeout interval has elapsed and there is an entry in the queue
    #     if diff_time.seconds > self.timeout_interval and queue_count > 1:
    #         self.start_interval_time = datetime.datetime.now()
    #         new_messages = self.has_new_messages(message_queue)
    #         if new_messages == 0:
    #             return
    #         print('new messages: {}'.format(new_messages))
    #         # get the next instance(s) to activate
    #         spinup_nodes = []
    #         for _ in range(1, new_messages+1):
    #             spinup_nodes.append(self.available_instances[self.instance_idx])
    #             self.instance_idx = (self.instance_idx + 1) % len(self.available_instances)
    #         self.start_n_instances(spinup_nodes)

    def scale_instances(self):
        # check if instance timed out
        self.check_timed_out()

        message_queue = self.sqs_client.get_queue_by_name(QueueName=self.queue_name)
        queue_count = int(message_queue.attributes['ApproximateNumberOfMessages'])
        print('Queue message count: {}'.format(queue_count))
        new_messages = self.has_new_messages(message_queue)
        # if new_messages == 0:
        #     return
        print('new messages: {}'.format(new_messages))
        while(new_messages > 0):
            stopped_instances = self.parse_ids(self.get_filtered_instances(['stopped']))
            # if no instances available, poll until they come available
            if len(stopped_instances) == 0:
                time.sleep(1)
                print('Polling for stopped nodes to become available')
                continue
            self.start_n_instances(stopped_instances[:new_messages])
            new_messages -= len(stopped_instances)


    # check queue to see if any of the messages available are new and return the count
    def has_new_messages(self, queue):
        # count the number of new messages and ignore old ones
        messages = queue.receive_messages(
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=10,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=0,
                WaitTimeSeconds=0
            )
        count = 0
        for message in messages:
            # debug
            print(message.message_id)
            # print(message)
            if message.message_id not in self.seen:
                self.seen.add(message.message_id)
                count += 1
        return count


    def get_filtered_instances(self, states):
        return self.ec2.instances.filter(
            Filters=[{'Name': 'instance-state-name', 'Values': states}])

    def check_timed_out(self):
        ids = []
        for id, t in self.nodes_timeout.items():
            diff = datetime.datetime.now() - t
            if diff.seconds > 360:
                ids.append(id)

        if len(ids) > 0:
            self.stop_n_instances(ids)


if __name__ == "__main__":
    # instance_ids = ['i-068951198664deb16', 'i-079621e32f3ce7a65', 'i-0919e7343c6cef68f', 'i-0a80bae146687a3f6']
    # instance_ids = []
    # for instance in get_filtered_instances(ec2, ['stopped']):
    #     instance_ids.append(instance.id)
    # testing
    # running instances
    controller = AwsEC2Controller(timeout_interval=1)

    # poll
    while True:
        controller.scale_instances()
        # controller.scale_instance_circular()
        # uncomment below to spin down
        # instances_to_stop = []
        # for id in controller.parse_ids(controller.get_filtered_instances(['running'])):
        #     if id != 'i-07d5c20bd4d482b06':
        #         instances_to_stop.append(id)
        # print(instances_to_stop)
        # controller.stop_n_instances(instances_to_stop)



    # for instance in controller.get_filtered_instances(['running']):
    #     print(instance.id, instance.instance_type)
    #     for tag in instance.tags:
    #         if tag['Key']=='Name':
    #             print(tag['Value'])

    # obj.start_instance("i-07df79accca04995b")
    # obj.stop_instance("i-07df79accca04995b")

