"""AMQP 0.9.1 Adapter to connect to RabbitMQ using pika library.

Publish and subscribe to queues and exchanges in RabbitMQ

"""
import pika
import functools
import threading

from message_queue import logger
from message_queue.adapters import BaseAdapter

LOGGER = logger.get(__name__)


class AMQPAdapter(BaseAdapter):
    __name__ = 'amqp'

    def __init__(self, host='localhost', port=5672, user='guest', password='guest', vhost='/'):
        """Create the connection credentials and parameters then connect.

        :param string host: Server host
        :param int port: Server port
        :param string user: Server server user
        :param string password: Server server password
        :param string vhost: Server virutal host

        """
        self.threads = []
        self.queue = None
        self._host = host
        self._credentials = pika.PlainCredentials(user, password)
        self._parameters = pika.ConnectionParameters(host, port, vhost, self._credentials)

        self.connect()

    def configurate_queue(self, **kwargs):
        """Configurate the queue.

        :param int prefetch_count: Specifies a prefetch window in terms of whole messages
        :param string queue: Queue name to connect
        :param bool passive: Only check to see if the queue exists
        :param bool dureble: Survive reboots of the broker
        :param bool exclusive: Only allow access by the current connection
        :param bool auto_delete: Delete after consumer cancels or disconnects
        :param bool arguments: Custom key/value arguments for the queue

        """
        if not self.queue:
            self.queue = kwargs.get('queue', '')
            self.basic_ack = kwargs.get('basic_ack', True)
            self.prefetch_count = kwargs.get('prefetch_count', 1)

            self.channel.queue_declare(
                queue       = self.queue,
                passive     = kwargs.get('passive', False),
                durable     = kwargs.get('durable', True),
                exclusive   = kwargs.get('exclusive', False),
                auto_delete = kwargs.get('auto_delete', False),
                arguments   = kwargs.get('arguments', None),
            )

            if self.prefetch_count > 0:
                self.channel.basic_qos(prefetch_count=self.prefetch_count)

            LOGGER.debug('Queue configured: queue=%r, basic_ack=%r, prefetch_count=%r',
                         self.queue, self.basic_ack, self.prefetch_count)

    def configurate_exchange(self, **kwargs):
        """Configurate the exchange.

        :param string exchange: Exchange name to connect
        :param string exchange_type: Exchange type

        """
        if not self.queue:
            self.queue = kwargs.get('exchange', '')

            self.channel.exchange_declare(
                exchange      = self.queue,
                exchange_type = kwargs.get('exchange_type', 'fanout')
            )

            LOGGER.debug('Exchange configured: exchange=%r', self.queue)

    def connect(self):
        """Connect to AMQP server usgin BlockingConnection.

        """
        try:
            self.connection = pika.BlockingConnection(self._parameters)
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()

            LOGGER.debug('Connected')

        except Exception as e:
            LOGGER.warning('Could not connect to host: %r', e)

            self.connect()

    def close(self):
        """Close connection and channel.

        """
        self.channel.close()
        self.connection.close()

        self.queue = self.connection = self.channel = None

    def send(self, message):
        """Publish a message in the queue.

        :param Message message: Message to publish in the channel

        """
        amqp_message = self.format_message(message.get_content())
        self.channel.basic_publish(**amqp_message)

    def format_message(self, message):
        """Format message to AMQP format.

        :param dict message: Message to format

        """
        exchange = message['properties'].get('exchange', '')
        delivery_mode = message['properties'].get('delivery_mode', 2)
        correlation_id = message['properties'].get('correlation_id', None)
        headers = message['properties'].get('headers', None)
        expiration = message['properties'].get('expiration', None)
        message_id = message['properties'].get('message_id', None)

        _message = {}
        _message['body'] = message['body']
        _message['routing_key'] = self.queue
        _message['exchange'] = exchange
        _message['properties'] = pika.BasicProperties(
            content_type='application/json',
            delivery_mode=delivery_mode,
            correlation_id=correlation_id,
            headers=headers,
            expiration=expiration,
            message_id=message_id,
        )

        LOGGER.debug('AMQP Message: %r ', _message)

        return _message

    def consume(self, worker):
        """Consume message from the queue.

        :param function worker: Method that consume the message

        """
        callback = functools.partial(self.consume_callback, worker=worker)
        self.channel.basic_consume(callback, self.queue)

        try:
            self.channel.start_consuming()

        except KeyboardInterrupt:
            self.channel.stop_consuming()

        for thread in self.threads:
            thread.join()

        self.close()

    def consume_callback(self, channel, method, properties, body, worker):
        """Create a new thred.

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver method: basic_deliver method
        :param pika.Spec.BasicProperties properties: properties
        :param str|unicode body: The message body
        :param function worker: Worker to execture in the consume callback
        """
        thread = threading.Thread(target=self.do_work, args=(channel, method, properties, body, worker))
        thread.start()
        self.threads.append(thread)

    def do_work(self, channel, method, properties, body, worker):
        """Execute worker

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver method: basic_deliver method
        :param pika.Spec.BasicProperties properties: properties
        :param str|unicode body: The message body
        :param function worker: Worker to execture in the consume callback
        """
        thread_id = threading.current_thread().ident
        tag = method.delivery_tag
        LOGGER.debug('Thread id: %r Delivery tag: %r Message body: %r', thread_id, tag, body)

        acknowledge = worker(channel, method, properties, body)
        callback = functools.partial(self._consume_acknowledge, channel, tag, acknowledge)
        self.connection.add_callback_threadsafe(callback)

    def _consume_acknowledge(self, channel, tag, acknowledge=True):
        """Message acknowledge.

        :param pika.channel.Channel channel: Channel to acknowledge the message
        :param int tag: Message tag to acknowledge
        :param bool acknowledge: If should acknowledge the message or not

        """
        if acknowledge is False:
            channel.basic_nack(delivery_tag=tag)
            return

        channel.basic_ack(delivery_tag=tag)

    def subscribe(self, exchange, queue, exchange_type="fanout", **kwargs):
        """Subscribes to a exchange.

        :param function worker: Method that consume the message
        :param string exchange: Exchange name
        :param string exchange: Queue name
        :param string exchange_type: Exchange type

        """
        self.queue = queue
        self.channel.exchange_declare(
            exchange=exchange, exchange_type=exchange_type)

        self.channel.queue_declare(
            queue=self.queue,
            passive=kwargs.get('passive', False),
            durable=kwargs.get('durable', True),
            exclusive=kwargs.get('exclusive', False),
            auto_delete=kwargs.get('auto_delete', False),
            arguments=kwargs.get('arguments', None)
        )

        self.channel.basic_qos(prefetch_count=kwargs.get('prefetch_count', 1))
        self.channel.queue_bind(exchange=exchange, queue=self.queue)
