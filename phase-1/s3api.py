import boto3 as bt
from botocore.exceptions import ClientError

# almost done need to check read from bucket function and customize for our project
class AwsS3Controller:
    # debug is just for debugging print statements
    # change to True for print statements to work
    def __init__(self, debug=False):
        self.debug = debug
        print("Aws S3 Controller Initialized")
        # resource is higher-level object-oriented service access
        # better abstraction, easier to comprehend "code"
        self.s3 = bt.resource("s3")
        # client is low-level service access
        # dictionary response,better performance
        self.s3_client = bt.client("s3")
        self.s3_bucket_names = ["input-bucket", "output-bucket"]
        self.prefix = "cloud-project1-team-any-"
        # ----------------------------#
        # function to create buckets  #
        # ----------------------------#

    def create_buckets(self, bucket_name="all", prefix=""):
        # waiters pause the execution of script until the status(here "bucket_exists") is reached.
        s3_waiter = self.s3_client.get_waiter("bucket_exists")

        def create(b):
            response = self.s3_client.create_bucket(Bucket=b)
            s3_waiter.wait(Bucket=b, WaiterConfig={"Delay": 10, "MaxAttempts": 10})
            if self.debug:
                print(response)

        if bucket_name == "all":
            print("Creating required buckets...")
            for name in self.s3_bucket_names:
                bkt_name = self.prefix + name
                if self.debug:
                    print("Creating bucket ", bkt_name)
                create(bkt_name)
        else:
            if self.debug:
                print("Creating bucket ", bucket_name)
            create(bucket_name)

            # ----------------------------#
            # function to delete buckets  #
            # ----------------------------#

    # need to add --> if no/0 buckets , need to notify user
    def delete_buckets(self, bucket_name="all", prefix=""):
        # waiters pause the execution of script until the status(here "bucket_not_exists") is reached.
        s3_waiter = self.s3_client.get_waiter("bucket_not_exists")

        def delete(b):
            response = self.s3_client.delete_bucket(Bucket=b)
            s3_waiter.wait(Bucket=b, WaiterConfig={"Delay": 10, "MaxAttempts": 10})
            if self.debug:
                print(response)

        bucket_list = self.s3.buckets.all()
        if bucket_name == "all":
            print("Deleting all the buckets")
            for name in self.s3_bucket_names:
                bucket_name = self.prefix + name
                # print(bucket_name)
                if self.s3.Bucket(bucket_name) in bucket_list:
                    if self.debug:
                        print("Deleting Bucket", bucket_name)
                    delete(bucket_name)
        else:
            if self.debug:
                print("Deleting bucket ", bucket_name)
            delete(bucket_name)

    def write_to_bucket(self, key, output_file, bucket_index):
        BKT_NAME = self.prefix + self.s3_bucket_names[bucket_index]
        # if the bucket does not exist, notify and create it.
        bucket_list = self.s3.buckets.all()
        if not self.s3.Bucket(BKT_NAME) in bucket_list:
            print("Bucket does not exist. Creating one with name ", BKT_NAME)
            self.create_buckets(BKT_NAME)
        print("Sending File ", key, " to the bucket ", BKT_NAME)

        s3_waiter = self.s3_client.get_waiter("object_exists")
        self.s3_client.upload_file(output_file, BKT_NAME, key)
        s3_waiter.wait(Bucket=BKT_NAME, Key=key)
        print("File succssefully uploaded..")

        # need to add debug para for logging

    def read_from_bucket(self, key, output_file):
        # we are reading from input bucket whose index according to our code is always zero
        BKT_NAME = self.prefix + self.s3_bucket_names[0]
        KEY = key
        print("Key: ", KEY, BKT_NAME)
        bucket_list = self.s3.buckets.all()
        # if bucket does not exists create one ////seems redundant
        if not self.s3.Bucket(BKT_NAME) in bucket_list:
            print("Bucket does not exist. Creating one with name ", BKT_NAME)
            self.create_buckets(BKT_NAME)
        # print(BKT_NAME)
        try:
            print("trying")
            self.s3.Bucket(BKT_NAME).download_file(KEY, output_file)
            print("done")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print("The object does not exist....")
            else:
                return False
        return True

    def get_bucket_list(self):
        blist = self.s3.buckets.all()
        for bucket in blist:
            print(bucket.name)


if __name__ == "__main__":
    obj = AwsS3Controller(debug=True)
    # key = "test"
    # obj.get_s3_bucket_list()
    # obj.create_buckets("all")
    # obj.get_s3_bucket_list()
    obj.delete_buckets()
    # key = "cse-asu-testkey"
    # obj.write_to_bucket(key, "./pi.pdf", 0)
    # obj.read_from_bucket(key, "./pi.pdf")

