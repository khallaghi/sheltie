import pika
import config
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue=config.QUEUE_CONFIG['response'], durable=True)


def send_message(message):
    print("SEND MESSAGE:")
    print(message.to_json())
    channel.basic_publish(exchange='',
                          routing_key=config.QUEUE_CONFIG['response'],
                          body=message.to_json(),
                          properties=pika.BasicProperties(
                             delivery_mode=2
                          ))
    connection.close()
