import json
from k8s.client import handle_request
from message import Message, Success, Failure
from messge_broker import receiver, send_message
import config
import pika


SUCCESS = 0
def callback(ch, method, properties, body):
    #try:
    print(" [x] Received %r" % body)
    pure_msg = json.loads(body)
    msg = Message(**pure_msg)
    response = handle_request(msg)
    #if response == SUCCESS:
    #    send_message(Success(msg.id))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    #except Exception as e:
        #print(e)
        #send_message(Failure(msg.id, message=str(e)))


# queue_name='command'
# credentials = pika.PlainCredentials(config.MQ_CONFIG['user'], config.MQ_CONFIG['pass'])
# parameters = pika.ConnectionParameters(host=config.MQ_CONFIG['host'],
#         port=config.MQ_CONFIG['port'],
#         virtual_host=config.MQ_CONFIG['vhost'],
#         credentials=credentials)
# connection = pika.BlockingConnection(parameters)
# channel = connection.channel()
#
# channel.queue_declare(queue=queue_name, durable=True)
#
# channel.basic_qos(prefetch_count=1)
# channel.basic_consume(callback,
#         queue=queue_name)
#
# channel.start_consuming()
if __name__ == '__main__':
    receiver(callback, config.QUEUE_CONFIG['command'])

