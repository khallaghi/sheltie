import pika
import config
import logging
FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s in function: %(func_name)s '

logger = logging.getLogger()
handler = logging.FileHandler(config.DEFAULT['LOG_PATH'])
formatter = logging.Formatter(FORMAT)
handler .setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)


def receiver(callback, queue_name):
    #try:
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
    #except Exception as e:
    #    extra = {
    #        'func_name': 'receiver'
    #    }
    #    logger.error("error_message: %s", e, extra=extra)

