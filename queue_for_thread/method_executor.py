import time
from .sqs import Sqs
from .logger import Logger


class MethodExecutor:
    class SqsException(Exception):
        pass

    @classmethod
    def execute(self, queue_name, function, before_action, aws_access_key_id, aws_secret_access_key, region_name, endpoint_url, polling_interval, log_level):
        client = Sqs(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            endpoint_url=endpoint_url
        )
        l = Logger(log_level=log_level)
        logger = l.get_logger()
        res_before_action = None if before_action is None else before_action()
        while True:
            try:
                message = client.receive_message(queue_name)
                body = message.body
            except Exception as err:
                logger.error(
                    'Error when receive message from SQS Queue = [%s]', queue_name)
                raise self.SqsException(err)
            if body is not None:
                logger.info(
                    'Got message [%s] from Queue Name = [%s]', body, queue_name)
                try:
                    function(sqs_message=body,
                             res_before_action=res_before_action)
                    message.delete()
                except Exception as err:
                    print('exepction')
                    logger.exception('Error in decorated function')
            time.sleep(polling_interval)
