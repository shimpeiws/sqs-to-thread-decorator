import time
from .sqs import Sqs


class MethodExecutor:
    class SqsException(Exception):
        pass

    def execute2(self, key):
        print("execute2")
        print(key)

    @classmethod
    def execute(self, function, queue_name, logger, **options):
        polling_interval = options.get('polling_interval', 3)
        aws_access_key_id = options.get('aws_access_key_id', '')
        aws_secret_access_key = options.get('aws_secret_access_key', '')
        region_name = options.get('region_name', '')
        endpoint_url = options.get('endpoint_url', '')
        client = Sqs(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url
        )

        while True:
            try:
                message = client.receive_message(queue_name)
            except Exception as err:
                logger.error(
                    'Error when receive message from SQS Queue = [%s]', queue_name)
                raise self.SqsException(err)
            if message is not None:
                logger.info(
                    'Got message [%s] from Queue Name = [%s]', message, queue_name)
                try:
                    function(sqs_message=message)
                except Exception as err:
                    logger.exception('Error in decorated function')
            time.sleep(polling_interval)
