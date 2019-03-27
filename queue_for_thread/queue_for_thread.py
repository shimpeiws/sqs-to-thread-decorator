import time
from concurrent.futures import ProcessPoolExecutor
from .sqs import Sqs
import logging
from logging import getLogger, StreamHandler, Formatter


class QueueForThread:
    class SqsException(Exception):
        pass

    def __init__(self, **options):
        self.functions = {}
        self.log_level = options.get('log_level', logging.INFO)
        self.logger = self.__init_logger()
        self.polling_interval = options.get('polling_interval', 3)
        self.aws_access_key_id = options.get('aws_access_key_id', '')
        self.aws_secret_access_key = options.get('aws_secret_access_key', '')
        self.region_name = options.get('region_name', '')
        self.endpoint_url = options.get('endpoint_url', '')
        try:
            self.client = Sqs(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
                endpoint_url=self.endpoint_url
            )
        except Exception as err:
            self.logger.error('Error when init SQS client')
            raise self.SqsException(err)

    def __init_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(self.log_level)
        stream_handler = StreamHandler()
        stream_handler.setLevel(self.log_level)
        handler_format = Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(handler_format)
        logger.addHandler(stream_handler)
        return logger

    def add_function(self, queue_name, function, **options):
        parallel_count = options.get('parallel_count', 1)
        self.functions[queue_name] = {
            'parallel_count': parallel_count,
            'function': function
        }

    def listen(self, queue_name, **options):
        def decorator(function):
            self.logger.info(
                'Start listen queue name = [%s] function name = [%s]', queue_name, function.__name__)
            self.add_function(queue_name, function, **options)
            return function
        return decorator

    def execute(self, queue_name):
        values = self.functions[queue_name]

        while True:
            try:
                message = self.client.receive_message(queue_name)
            except Exception as err:
                # self.logger.error(
                #     'Error when receive message from SQS Queue = [%s]', queue_name)
                raise self.SqsException(err)
            if message is not None:
                # self.logger.info(
                #     'Got message [%s] from Queue Name = [%s]', message, queue_name)
                try:
                    values['function'](sqs_message=message)
                except Exception as err:
                    print("ERROR")
                    # self.logger.exception('Error in decorated function')
            time.sleep(self.polling_interval)

    def start(self):
        self.logger.info('QueueForThread Start!!!')
        for key in self.functions:
            values = self.functions[key]
            parallel_count = values['parallel_count']
            with ProcessPoolExecutor(max_workers=parallel_count) as executor:
                self.logger.info(
                    'Start Queue = [%s] with Parallel Count = [%d]', key, parallel_count)
                queue_name_arr = [
                    key for i in range(parallel_count)]
                executor.map(self.execute, queue_name_arr)
