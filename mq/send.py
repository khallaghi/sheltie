import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='command', durable=True)


def send_message(message):
    channel.basic_publish(exchange='',
                          routing_key='response',
                          body=message.to_json(),
                          properties=pika.BasicProperties(
                             delivery_mode=2
                          ))
    connection.close()
