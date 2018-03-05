import json
from k8s.client import handle_request
from message import Message, Success, Failure
from message_broker import receiver, send_message
import config

SUCCESS = 0


def callback(ch, method, properties, body):
    msg = None
    try:
        print(" [x] Received %r" % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        pure_msg = json.loads(body)
        msg = Message(**pure_msg)
        handle_request(msg)
        send_message(Success(msg.id))
        print('DONE')
    except Exception as e:
        print(e)
        send_message(Failure(msg.id, message=str(e)))


if __name__ == '__main__':
    receiver(callback, config.QUEUE_CONFIG['command'])

