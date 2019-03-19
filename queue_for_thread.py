import time
from concurrent.futures import ProcessPoolExecutor
from sqs import Sqs


class QueueForThread:
    def __init__(self, **options):
        self.functions = {}
        self.polling_interval = options.get('polling_interval', 3)
        self.aws_access_key_id = options.get('aws_access_key_id', '')
        self.aws_secret_access_key = options.get('aws_secret_access_key', '')
        self.region_name = options.get('region_name', '')
        self.endpoint_url = options.get('endpoint_url', '')

    def add_function(self, queue_name, function, **options):
        parallel_count = options.get('parallel_count', 1)
        self.functions[queue_name] = {
            'parallel_count': parallel_count,
            'function': function
        }

    def listen(self, queue_name, **options):
        def decorator(function):
            self.add_function(queue_name, function, **options)
            return function
        return decorator

    def execute(self, queue_name):
        values = self.functions[queue_name]
        while True:
            client = Sqs(
              aws_access_key_id=self.aws_access_key_id,
              aws_secret_access_key=self.aws_secret_access_key,
              region_name=self.region_name,
              endpoint_url=self.endpoint_url
            )
            message = client.receive_message(queue_name)
            if message is not None:
                values['function'](sqs_message=message)
            time.sleep(self.polling_interval)

    def start(self):
        for key in self.functions:
            values = self.functions[key]
            parallel_count = values['parallel_count']
            with ProcessPoolExecutor(max_workers=parallel_count) as executor:
                queue_name_arr = [
                    key for i in range(parallel_count)]
                executor.map(self.execute, queue_name_arr)
