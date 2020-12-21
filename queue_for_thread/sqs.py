import os
import boto3
from threading import Lock


class Sqs():
    __instance = None

    def __init__(self, **options):
        self.lock = Lock()
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID'] if 'AWS_ACCESS_KEY_ID' in os.environ.keys() else options.get(
            'aws_access_key_id', '')
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY'] if 'AWS_SECRET_ACCESS_KEY' in os.environ.keys() else options.get(
            'aws_secret_access_key', '')
        region_name = os.environ['AWS_REGION_NAME'] if 'AWS_REGION_NAME' in os.environ.keys() else options.get(
            'region_name', '')
        endpoint_url = os.environ['SQS_URL'] if 'SQS_URL' in os.environ.keys() else options.get(
            'endpoint_url', '')
        self.sqs = boto3.resource('sqs',
                                  aws_access_key_id=aws_access_key_id,
                                  aws_secret_access_key=aws_secret_access_key,
                                  region_name=region_name,
                                  endpoint_url=endpoint_url,
                                  )

    def create_queue(self, queue_name):
        return self.sqs.create_queue(
            QueueName=queue_name
        )

    def fetch_queue(self, queue_name):
        return self.sqs.get_queue_by_name(QueueName=queue_name)

    def send_message(self, queue_name, messages):
        request = messages if isinstance(messages, list) else [messages]
        queue = self.fetch_queue(queue_name)
        return queue.send_messages(Entries=(request))

    def receive_message(self, queue_name):
        queue = self.fetch_queue(queue_name)
        with self.lock:
            r = queue.receive_messages(MaxNumberOfMessages=1)
            if len(r) == 0:
                return None
            else:
                message = r[0]
                # message.set_attributes(
                #     VisibilityTimeout=60
                # )
                return message
