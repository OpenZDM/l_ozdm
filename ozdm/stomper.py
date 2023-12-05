import abc
import logging
import threading
import time
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from ozdm import avroer
import avro.schema
from typing import Dict, List

class MessageListener(abc.ABC):
    @abc.abstractmethod
    def on_message(self, subject: avroer.AvroObject) -> None:
        pass

class TopicKey:
    def __init__(self, topic: str, listen_schema_name: str | None):
        self.topic = topic
        self.listen_schema_name = listen_schema_name

    def __eq__(self, other):
        return isinstance(other, TopicKey) and self.topic == other.topic and self.listen_schema_name == other.listen_schema_name

    def __hash__(self):
        return hash((self.topic, self.listen_schema_name))

class TopicValue:
    def __init__(self, topic: str, listen_schema_name: str, schema: avro.schema.Schema, observer: MessageListener):
        self.topic = topic
        self.listen_schema_name = listen_schema_name
        self.schema = schema
        self.observer = observer

    def __eq__(self, other):
        return isinstance(other, TopicValue) and self.topic == other.topic and self.listen_schema_name == other.listen_schema_name

class ProtonHandler(MessagingHandler):
    def __init__(self, server_url, user, password, auto_reconnect, logger=None):
        super(ProtonHandler, self).__init__()
        self.server_url = server_url
        self.user = user
        self.password = password
        self.auto_reconnect = auto_reconnect
        self.logger = logger or logging.root
        self.connection = None
        self.sender = None
        self.topic_listeners = {}
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5

    def on_start(self, event):
        self.connect(event.container)

    def connect(self, container):
        try:
            self.connection = container.connect(self.server_url, user=self.user, password=self.password)
            self.sender = container.create_sender(self.connection, None)
            self.reconnect_attempts = 0
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            if self.auto_reconnect and self.reconnect_attempts < self.max_reconnect_attempts:
                self.reconnect_attempts += 1
                time.sleep(5)
                self.connect(container)

    def on_disconnected(self, event):
        if self.auto_reconnect and self.reconnect_attempts < self.max_reconnect_attempts:
            self.logger.info("Attempting to reconnect...")
            self.reconnect_attempts += 1
            self.connect(event.container)
        else:
            self.logger.error("Maximum reconnect attempts reached. Giving up.")

    def send_message(self, topic, avro_object):
        if not self.sender:
            self.logger.error("Sender not established. Cannot send message.")
            return
        serialize = avroer.AvroSerializer(schema=avro_object.schema)
        content = serialize(content=avro_object.data)
        message = Message(address=topic, body=content, properties={"schema": avro_object.schema.get_prop("name")})
        self.sender.send(message)
        self.logger.info(f"Message sent to topic {topic}")

    def on_message(self, event):
        self.logger.info(f"Received message on AMQP topic: {event.receiver.source.address}")
        try:
            topic = event.receiver.source.address
            observers = []
            for key, value in self.topic_listeners.items():
                if key.topic == topic:
                    observers.extend(value)

            payload = event.message.body
            deserialize = avroer.AvroDeserializer()
            inferred_schema, data = deserialize(payload=payload)

            for d in data:
                avro_object = avroer.AvroObject(schema=inferred_schema, data=d)
                for observer in observers:
                    observer.observer.on_message(avro_object)
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema = None,
                  listen_schema_name: str = None):
        topic_key = TopicKey(topic, listen_schema_name)
        topic_value = TopicValue(topic, listen_schema_name, schema, observer)

        if topic_key not in self.topic_listeners:
            self.topic_listeners[topic_key] = []

        if topic_value not in self.topic_listeners[topic_key]:
            self.topic_listeners[topic_key].append(topic_value)
            self.logger.info(f"Subscribed to topic: {topic} with schema: {listen_schema_name}")

            if self.connection:
                try:
                    receiver = self.connection.create_receiver(topic)
                    self.logger.info(f"Proton receiver created for topic: {topic}")
                except Exception as e:
                    self.logger.error(f"Failed to create Proton receiver for topic {topic}: {e}")
            else:
                self.logger.error("No active connection to create Proton receiver.")
        else:
            self.logger.info(f"Already subscribed to topic: {topic} with schema: {listen_schema_name}")

class AvroStomper:
    def __init__(self, host, port, user=None, password=None, auto_reconnect=True, logger=None):
        self.logger = logger or logging.root
        self.handler = ProtonHandler(f'amqp://{user}:{password}@{host}:{port}', user, password, auto_reconnect, self.logger)
        self.container = Container(self.handler)
        self.thread = threading.Thread(target=self.container.run)
        self.thread.start()

    def connect(self):
        if not self.thread.is_alive():
            self.thread.start()

    def disconnect(self):
        if self.container and self.container.running:
            self.container.stop()
        if self.thread.is_alive():
            self.thread.join()

    def send(self, topic: str, avro_object: avroer.AvroObject) -> None:
        self.handler.send_message(topic, avro_object)

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema=None,
                  listen_schema_name: str = None) -> None:
        self.handler.subscribe(observer, topic, schema, listen_schema_name)
