import json
from k8s.client import handle_request
from message import Message, Success, Failure
from message_broker import Consumer, send_message
import config


def callback(channel, method, body):
    _id = -1
    try:
        print(" [x] Received %r" % body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        pure_msg = json.loads(body)
        msg = Message(**pure_msg)
        _id = msg.id
        handle_request(msg)
        #send_message(Success(str(_id)))
        print('DONE')
    except Exception as e:
        print(e)
        send_message(Failure(str(_id), message=str(e)))


if __name__ == '__main__':
    c = Consumer(config.QUEUE_CONFIG['command'], callback)
    c.run()
    #receiver(config.QUEUE_CONFIG['command'], callback)

