from queue_for_thread import QueueForThread
import time

# Init QueueForThread
#
# app = QueueForThread(
#     aws_access_key_id="YOUR_AWS_ACCESS_KEY_ID",
#     aws_secret_access_key="AWS_ACCESS_KEY_ID",
#     region_name="REGION_NAME",
#     endpoint_url="ENDPOINT_URL",
# )
app = QueueForThread()


# Use decorator @app.lesten(queue_name, options)
# queue_name = queue name for listen
# options:
#  parallel_count -> set process count
@app.listen('test', parallel_count=10)
#  This is your own method. You can change method name.
# method_name(**options)
# options contains "sqs_message", you can use value from SQS
def execute(sqs_message="", **options):
    print('!!!main.py From Execute!!!')
    print('!!!sqs_message!!!', sqs_message)
    time.sleep(10)


if __name__ == '__main__':
    print("main loop")
    # Please call `app.start()`
    app.start()
