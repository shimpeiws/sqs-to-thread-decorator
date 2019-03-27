import time
from concurrent.futures import ProcessPoolExecutor
from .sqs import Sqs
from .logger import Logger
from .method_executor import MethodExecutor
from logging import INFO


class QueueForThread:
    class SqsException(Exception):
        pass

    def __init__(self, **options):
        self.functions = {}
        log_level = options.get('log_level', INFO)
        logger = Logger(log_level=log_level)
        self.logger = logger.get_logger()
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

    def options(self):
        return {
            'polling_interval': self.polling_interval,
            'aws_access_key_id': self.aws_access_key_id,
            'aws_secret_access_key': self.aws_secret_access_key,
            'region_name': self.region_name,
            'endpoint_url': self.endpoint_url
        }

    def start(self):
        self.logger.info('QueueForThread Start!!!')
        for key in self.functions:
            values = self.functions[key]
            parallel_count = values['parallel_count']
            with ProcessPoolExecutor(max_workers=parallel_count) as executor:
                self.logger.info(
                    'Start Queue = [%s] with Parallel Count = [%d]', key, parallel_count)
                key_arr = [
                    key for i in range(parallel_count)]
                function_arr = [
                    values['function'] for i in range(parallel_count)]
                aws_access_key_id_arr = [
                    self.aws_access_key_id for i in range(parallel_count)]
                aws_secret_access_key_arr = [
                    self.aws_secret_access_key for i in range(parallel_count)]
                region_name_arr = [
                    self.region_name for i in range(parallel_count)]
                endpoint_url_arr = [
                    self.endpoint_url for i in range(parallel_count)]
                polling_interval_arr = [
                    self.polling_interval for i in range(parallel_count)]
                executor.map(MethodExecutor.execute, key_arr,
                             function_arr, aws_access_key_id_arr, aws_secret_access_key_arr, region_name_arr, endpoint_url_arr, polling_interval_arr)
