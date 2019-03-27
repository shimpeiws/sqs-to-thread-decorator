import time
from .sqs import Sqs


class MethodExecutor:
    class SqsException(Exception):
        pass

    def __init__(self, function, logger, **options):
        # self.function = function
        # self.logger = logger
        # self.polling_interval = options.get('polling_interval', 3)
        try:
            print('init')
            # self.client = Sqs(
            #     aws_access_key_id=options.get('aws_access_key_id', ''),
            #     aws_secret_access_key=options.get('aws_secret_access_key', ''),
            #     region_name=options.get('region_name', ''),
            #     endpoint_url=options.get('endpoint_url', '')
            # )
        except Exception as err:
            # self.logger.error('Error when init SQS client')
            raise self.SqsException(err)

    def execute(self, queue_name):
        while True:
            print('execute')
            # try:
            #     message = self.client.receive_message(queue_name)
            # except Exception as err:
            #     self.logger.error(
            #         'Error when receive message from SQS Queue = [%s]', queue_name)
            #     raise self.SqsException(err)
            # if message is not None:
            #     self.logger.info(
            #         'Got message [%s] from Queue Name = [%s]', message, queue_name)
            #     try:
            #         self.function(sqs_message=message)
            #     except Exception as err:
            #         logger.exception('Error in decorated function')
            time.sleep(self.polling_interval)
