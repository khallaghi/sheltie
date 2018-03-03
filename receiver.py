import pika
import json
from k8s.client import handle_request
credentials = pika.PlainCredentials('test', 'test')
parameters = pika.ConnectionParameters(host='localhost',
                                       port=5672,
                                       virtual_host='/',
                                       credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.queue_declare(queue='command', durable=True)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    msg = json.loads(body)
    handle_request(msg)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='command')

channel.start_consuming()
