from sqs import Sqs
import time

client = Sqs()

queue_name = 'test'

queue = client.fetch_queue(queue_name)

for i in range(50):
    message_body = 'message at %s' % time.time()
    client.send_message(queue_name, {'Id': '123', 'MessageBody': message_body})
