import pika
import config
import logging
import threading
# FORMAT = '%(asctime)s %(name)-12s %(levelname)-8s in function: %(func_name)s '

# logger = logging.getLogger()
# handler = logging.FileHandler(config.DEFAULT['LOG_PATH'])
# formatter = logging.Formatter(FORMAT)
# handler .setFormatter(formatter)
# logger.addHandler(handler)
# logger.setLevel(logging.ERROR)


logging.basicConfig()


class Consumer(object):
    """
    A RabbitMQ topic exchange consumer that will call the specified function
    when a message is received.
    """

    def __init__(self, exchange, callable_func):
        """
        Create a consumer instance and connection to RabbitMQ.
        """
        self.exchange = exchange
        self.callable = callable_func
        self.queue = ''
        self.type = 'direct'
        self.channel = None
        self.consumer_tag = None

        credentials = pika.PlainCredentials(config.MQ_CONFIG['user'], config.MQ_CONFIG['pass'])
        self.parameters = pika.ConnectionParameters(
            host=config.MQ_CONFIG['host'],
            port=config.MQ_CONFIG['port'],
            virtual_host=config.MQ_CONFIG['vhost'],
            credentials=credentials,
            heartbeat_interval=config.MQ_CONFIG['heartbeat_interval'])
        self.connection = pika.SelectConnection(self.parameters,
                                                self.on_connected)

    def on_connected(self, connection):
        """
        Called by pika when a connection is established.
        """
        connection.channel(self.on_channel_open)

    def on_channel_open(self, channel):
        """
        Called by pika when the channel is opened.
        """
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.exchange_declare(self.on_exchange_declareok,
                                      exchange=self.exchange,
                                      exchange_type=self.type)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """
        Called by pika when the channel is closed.
        """
        self.connection.close()

    def on_consumer_cancelled(self, frame):
        """
        Called by pika when the RabbitMQ connection is lost.
        """
        self.channel.close()

    def on_exchange_declareok(self, frame):
        """
        Called by pika when RabbitMQ has finished the Exchange.Declare
        command.
        """
        self.channel.queue_declare(self.on_queue_declareok, self.queue, exclusive=True)

    def on_queue_declareok(self, frame):
        """
        Called by pika when RabbitMQ has finished the Queue.Declare
        command.
        """
        # Get the server assigned queue name
        self.queue = frame.method.queue
        self.channel.queue_bind(self.on_bindok, queue=self.queue, exchange=self.exchange, routing_key="#")

    def on_bindok(self, frame):
        """
        Called by pika when RabbitMQ has finished the Queue.Bind command.
        Now it's safe to start consuming messages.
        """
        self.start_consuming()

    def start_consuming(self):
        """
        Start consuming messages.
        """
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.consumer_tag = self.channel.basic_consume(self.on_message,
                                                       self.queue)

    def stop_consuming(self):
        """
        Stop consuming messages.
        """
        self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

    def on_cancelok(self, frame):
        """
        Called by pika when RabbitMQ acknowledges the cancellation of a
        consumer.
        """
        self.connection.close()

    def on_message(self, channel, method, properties, body):
        """
        Called by pika when a message is delivered from RabbitMQ.  Call
        the specified function.
        """
        if self.callable:
            self.callable(channel, method, body)

    def run(self):
        """
        Start the consumer event processing loop.
        """
        try:
            self.connection.ioloop.start()

        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """
        Stop the event processing loop.
        """
        # Close the connection and restart the ioloop to allow the
        # process to terminate.
        self.stop_consuming()
        self.connection.ioloop.start()







# def main_task1(ch, method, body):
#     _id = -1
#     try:
#         print(" [x] Received %r" % body)
#         ch.basic_ack(delivery_tag=method.delivery_tag)
#         pure_msg = json.loads(body)
#         msg = Message(**pure_msg)
#         _id = msg.id
#         handle_request(msg)
#         send_message(Success(str(_id)))
#         print('DONE')
#     except Exception as e:
#         print(e)
#         send_message(Failure(str(_id), message=str(e)))


# def receiver(queue_name, main_task):
#
#     credentials = pika.PlainCredentials(config.MQ_CONFIG['user'], config.MQ_CONFIG['pass'])
#     parameters = pika.ConnectionParameters(host=config.MQ_CONFIG['host'],
#                                            port=config.MQ_CONFIG['port'],
#                                            virtual_host=config.MQ_CONFIG['vhost'],
#                                            credentials=credentials,
#                                            heartbeat_interval=config.MQ_CONFIG['heartbeat_interval'])
#     connection = pika.BlockingConnection(parameters)
#     connection.process_data_events()
#     channel = connection.channel()
#
#     channel.queue_declare(queue=queue_name, durable=True)
#
#     channel.basic_qos(prefetch_count=1)
#     channel.confirm_delivery()
#
#     def thread_func(ch, method, body):
#         main_task(ch, method, body)
#         #ch.basic_ack(delivery_tag = method.delivery_tag)
#
#     def callback(ch, method, properties, body):
#         threading.Thread(target=thread_func, args=(ch, method, body)).start()
#
#     channel.basic_consume(callback,
#                           queue=queue_name)
#
#     channel.start_consuming()

