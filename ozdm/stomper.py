import abc
import logging
from typing import Dict, List

import avro.schema
from proton._message import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from ozdm import avroer
from ozdm.avroer import AvroSerializer, AvroDeserializer


class MessageListener(abc.ABC):
    @abc.abstractmethod
    def on_message(self, subject: avroer.AvroObject) -> None:
        pass

# class ReconnectListener(MessagingHandler):
#     def __init__(self, container, host, port, user, password, logger, connection_callback):
#         super().__init__()
#         self.container = container
#         self.host = host
#         self.port = port
#         self.user = user
#         self.password = password
#         self.logger = logger or logging.root
#         self.connection = None
#         self.connection_callback = connection_callback
#
#     def on_error(self, event):
#         self.logger.error('Received an error: %s' % event.message.body)
#
#     def on_message(self, event):
#         self.logger.info('Received a message: %s' % event.message.body)
#
#     def connect(self):
#         if not self.connection:
#             try:
#                 self.logger.info("Attempting to connect...")
#                 conn_url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"
#                 self.connection = self.container.connect(conn_url, reconnect=[10, 10, 10])  # Example intervals
#                 self.container.create_sender(self.connection, None)
#                 self.logger.info(f"Connecting to {conn_url}")
#             except Exception as e:
#                 self.logger.error(f"Error in connection: {e}", exc_info=True)
#                 self.connection = None
#
#     def on_start(self, event):
#         if self.user and self.password:
#             event.container.sasl_enabled = True
#             event.container.allowed_mechs = "PLAIN"
#         self.connect()
#
#     def on_connection_opened(self, event):
#         self.logger.info("Connection opened")
#         if self.connection_callback:
#             self.connection_callback(event.connection)
#         # Create a sender when the connection is opened
#         try:
#             self.sender = event.container.create_sender(event.connection, None)  # Update target as needed
#         except Exception as e:
#             self.logger.error(f"Error creating sender: {e}", exc_info=True)
#
#     def on_disconnected(self, event):
#         self.logger.info("Disconnected")
#         self.connection_callback(None)

class TopicValue:
    def __init__(self, topic: str, listen_schema_name: str, schema: avro.schema.Schema, observer: MessageListener):
        self.topic = topic
        self.listen_schema_name = listen_schema_name
        self.schema = schema
        self.observer = observer

    def __eq__(self, other):
        if isinstance(other, TopicValue):
            return self.topic == other.topic and self.listen_schema_name == other.listen_schema_name
        return False

class TopicKey:
    def __init__(self, topic: str, listen_schema_name: str | None):
        self.topic = topic
        self.listen_schema_name = listen_schema_name

    def __eq__(self, other):
        if isinstance(other, TopicKey):
            return self.topic == other.topic and self.listen_schema_name == other.listen_schema_name
        return False

    def __hash__(self):
        return hash((self.topic, self.listen_schema_name))

    def __ne__(self, other):
        return not (self == other)

class TopicListener(MessagingHandler):
    _observers: Dict[TopicKey, List[TopicValue]] = {}

    def __init__(self, logger=None):
        super().__init__()
        self.logger = logger or logging.root

    def on_message(self, event):
        message = event.message
        topic = message.subject
        schema_name = message.properties.get("schema")
        observers = []

        k = TopicKey(topic=topic, listen_schema_name=schema_name)
        if k in self._observers:
            observers.extend(self._observers[k])

        k = TopicKey(topic=topic, listen_schema_name=None)
        if k in self._observers:
            observers.extend(self._observers[k])

        payload = message.body
        deserialize = avroer.AvroDeserializer()
        inferred_schema, data = deserialize(payload=payload)

        for d in data:
            avro_object = avroer.AvroObject(schema=inferred_schema, data=d)
            for observer in observers:
                observer.on_message(avro_object)

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema,
                  listen_schema_name: str = None) -> None:
        k = TopicKey(topic=topic, listen_schema_name=listen_schema_name)
        if k not in self._observers.keys():
            self._observers[k] = []
        self._observers[k].append(TopicValue(topic=topic,
                                             listen_schema_name=listen_schema_name,
                                             schema=schema,
                                             observer=observer))

class AvroStomper(MessagingHandler):
    def __init__(self, host, port, user, password, logger=None):
        super(AvroStomper, self).__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self.url = f"amqp://{user}:{password}@{host}:{port}"
        self.sender = None
        self.connected = False
        self.container = None
        self.topic = None

    def connect(self, topic=None):
        if not self.connected:
            self.topic = topic
            self.container = Container(self)
            self.container.run()

    def on_start(self, event):
        self.logger.info("Starting AvroStomper...")
        try:
            conn = event.container.connect(
                url=self.url,
                sasl_enabled=True,
                allowed_mechs="PLAIN",
                reconnect=[10,10,10]
            )
            if self.topic:
                self.sender = event.container.create_sender(conn, self.topic)
            self.connected = True
            self.logger.info(f"Connected to AMQP server at {self.url}")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            self.connected = False

    def send(self, topic, avro_object):
        if not self.connected or self.sender is None:
            self.logger.error("Not connected or sender not initialized. Attempting to connect...")
            self.connect(topic)

        if self.connected:
            if self.sender is None or self.topic != topic:
                self.sender = self.container.create_sender(self.connection, topic)
                self.topic = topic
                self.logger.info(f"Sender switched to topic: {topic}")

            try:
                serializer = AvroSerializer(schema=avro_object.schema)
                content = serializer(content=avro_object.data)
                message = Message(subject=topic, body=content)
                self.sender.send(message)
                self.logger.info(f"Message sent to {topic}")
            except Exception as e:
                self.logger.error(f"Error sending message: {e}")

    def disconnect(self):
        if self.sender:
            self.sender.close()
        if self.container:
            self.container.stop()
        self.connected = False
        self.logger.info("AvroStomper disconnected.")


    # def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema = None,
    #               listen_schema_name: str = None):
    #     self.topic_listener.subscribe(observer=observer, topic=topic, schema=schema,
    #                                   listen_schema_name=listen_schema_name)
    #     self.reconnect_listener.subscribed(topic)



# class SimpleQueueProducer(MessagingHandler):
#     def __init__(self, server_url, queue_name):
#         super(SimpleQueueProducer, self).__init__()
#         self.server_url = server_url
#         self.queue_name = queue_name
#         self.connection = None
#
#     def on_start(self, event):
#         print("Connection established to", self.server_url)
#         self.connection = event.container.connect(self.server_url)
#
#     def on_connection_opened(self, event):
#         print("Connection opened and ready for use")
#         self.sender = event.container.create_sender(self.connection, self.queue_name)
#
#     def on_sendable(self, event):
#         message = Message(body="Hello, World!")
#         self.sender.send(message)
#         print("Message sent")
#         event.connection.close()
#
# def main():
#     server_url = 'amqp://artemis:artemis@artemis:61616'
#     queue_name = 'example_queue'
#
#     producer = SimpleQueueProducer(server_url, queue_name)
#     Container(producer).run()
#
# if __name__ == "__main__":
#     main()


# def main():
#     host = "localhost"  # Update with your AMQP server's host
#     port = 61616  # Update with your AMQP server's port
#     user = "artemis"  # Update with your credentials
#     password = "artemis"  # Update with your credentials
#
#     # Set up logging
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger(__name__)
#
#     # Create AvroStomper instance
#     avro_stomper = AvroStomper(host, port, user, password, logger)
#
#     # Connect AvroStomper
#     avro_stomper.connect()
#
#     # Send a simple text message
#     try:
#         test_message = Message(body="Hello, World!")
#         avro_stomper.send("test_topic", test_message)
#         print("Message sent successfully.")
#     except Exception as e:
#         logger.error(f"Failed to send message: {e}")
#
#     # Disconnect
#     avro_stomper.disconnect()
#
# if __name__ == "__main__":
#     main()


# def create_large_message(size_kb=100):
#     """
#     Create a large message of specified size in KB.
#     """
#     message_size = size_kb * 1024  # Convert KB to bytes
#     message_content = "x" * message_size  # Create a string with repeated characters
#     return message_content
#
# def main():
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger(__name__)
#
#     host = "localhost"
#     port = 61616
#     user = "artemis"
#     password = "artemis"
#     topic = "test_topic"
#
#     stomper = AvroStomper(host, port, user, password, logger)
#
#     stomper.connect(topic)
#
#     # Send messages
#     for _ in range(10000):
#         large_message = create_large_message()
#         message = Message(body=large_message)
#         stomper.send(topic, message)
#         logger.info(f"Sent message of size {len(large_message)} bytes")
#
#     stomper.disconnect()
#
# if __name__ == "__main__":
#     main()