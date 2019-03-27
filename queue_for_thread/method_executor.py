import time
from .sqs import Sqs


class MethodExecutor:
    class SqsException(Exception):
        pass

    @classmethod
    def execute(self, queue_name, function, aws_access_key_id, aws_secret_access_key, region_name, endpoint_url, polling_interval):
        while True:
            print('execute')
            print(queue_name)
            print(function.__name__)
            client = Sqs(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                endpoint_url=endpoint_url
            )
            try:
                message = client.receive_message(queue_name)
            except Exception as err:
                # self.logger.error(
                    # 'Error when receive message from SQS Queue = [%s]', queue_name)
                raise self.SqsException(err)
            if message is not None:
                # self.logger.info(
                    # 'Got message [%s] from Queue Name = [%s]', message, queue_name)
                try:
                    self.function(sqs_message=message)
                except Exception as err:
                    print('exepction')
                    # logger.exception('Error in decorated function')
            time.sleep(polling_interval)
