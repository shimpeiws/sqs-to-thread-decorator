from queue_for_thread import QueueForThread
import time
import datetime
import random

# Init QueueForThread
#
# app = QueueForThread(
#     aws_access_key_id="YOUR_AWS_ACCESS_KEY_ID",
#     aws_secret_access_key="AWS_ACCESS_KEY_ID",
#     region_name="REGION_NAME",
#     endpoint_url="ENDPOINT_URL",
#     polling_interval=1,
#     log_level=logging.ERROR,
# )
app = QueueForThread()


def before_action():
    print('!!!this is before action!!!')
    return {"foo": 'bar'}


ba = before_action

# Use decorator @app.lesten(queue_name, options)
# queue_name = queue name for listen
# options:
#  parallel_count -> set process count
@app.listen('test', parallel_count=10, before_action=ba)
#  This is your own method. You can change method name.
# method_name(**options)
# options contains "sqs_message", you can use value from SQS
def execute(sqs_message="", res_before_action="", **options):
    print('!!!main.py From Execute!!!')
    print('!!!sqs_message!!!', sqs_message)
    print('!!!res_before_action!!!', res_before_action)
    if random.randrange(10) % 5 == 0:
        raise Exception('some error here')
    time.sleep(1)


if __name__ == '__main__':
    print("main loop")
    # Please call `app.start()`
    app.start()
