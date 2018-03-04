import pika
import config


def receiver(callback, queue_name):
    credentials = pika.PlainCredentials(config.MQ_CONFIG['user'], config.MQ_CONFIG['pass'])
    parameters = pika.ConnectionParameters(host=config.MQ_CONFIG['host'],
                                           port=config.MQ_CONFIG['port'],
                                           virtual_host=config.MQ_CONFIG['vhost'],
                                           credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback,
                          queue=queue_name)

    channel.start_consuming()
