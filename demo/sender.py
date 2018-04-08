import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


# message = ' '.join(sys.argv[1:]) or "Hello World!"
# channel.basic_publish(exchange='',
#                       routing_key='command',
#                       body=message,
#                       properties=pika.BasicProperties(
#                          delivery_mode=2
#                       ))


channel.exchange_declare(exchange='command',
                         exchange_type='direct')

message = ' '.join(sys.argv[1:]) or "info: Hello World!"
channel.basic_publish(exchange='command',
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()
