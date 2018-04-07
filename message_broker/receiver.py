import pika
import config
import logging
import threading
FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s in function: %(func_name)s '

logger = logging.getLogger()
handler = logging.FileHandler(config.DEFAULT['LOG_PATH'])
formatter = logging.Formatter(FORMAT)
handler .setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)

def main_task1(ch, method, body):
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

def receiver(queue_name, main_task):
    #try:
    
    credentials = pika.PlainCredentials(config.MQ_CONFIG['user'], config.MQ_CONFIG['pass'])
    parameters = pika.ConnectionParameters(host=config.MQ_CONFIG['host'],
                                           port=config.MQ_CONFIG['port'],
                                           virtual_host=config.MQ_CONFIG['vhost'],
                                           credentials=credentials,
                                           heartbeat_interval=config.MQ_CONFIG['heartbeat_interval'])
    connection = pika.BlockingConnection(parameters)
    connection.process_data_events()
    channel = connection.channel()


    channel.queue_declare(queue=queue_name, durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.confirm_delivery()
    #channel.process_data_events()


    def thread_func(ch, method, body):
        main_task(ch, method, body)
        #ch.basic_ack(delivery_tag = method.delivery_tag)

    def callback(ch, method, properties, body):
        threading.Thread(target=thread_func, args=(ch, method, body)).start()

    channel.basic_consume(callback,
                          queue=queue_name)

    channel.start_consuming()
    #except Exception as e:
    #    extra = {
    #        'func_name': 'receiver'
    #    }
    #    logger.error("error_message: %s", e, extra=extra)

