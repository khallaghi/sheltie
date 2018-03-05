import json
from k8s.client import handle_request
from message import Message, Success, Failure
from message_broker import receiver, send_message
import config

SUCCESS = 0


def callback(ch, method, properties, body):
    _id = -1
    try:
        print(" [x] Received %r" % body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        pure_msg = json.loads(body)
        msg = Message(**pure_msg)
        _id = msg.id
        handle_request(msg)
        send_message(Success(str(_id)))
        print('DONE')
    except Exception as e:
        print(e)
        send_message(Failure(str(_id), message=str(e)))



if __name__ == '__main__':
    receiver(callback, config.QUEUE_CONFIG['command'])

