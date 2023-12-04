import logging
import threading
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container
from ozdm import avroer
import avro.schema
import abc
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
        if isinstance(other, TopicKey):
            return self.topic == other.topic and self.listen_schema_name == other.listen_schema_name
        return False

    def __hash__(self):
        return hash((self.topic, self.listen_schema_name))

class ProtonHandler(MessagingHandler):
    def __init__(self, server_url, user, password, topic_listeners, logger=None):
        super(ProtonHandler, self).__init__()
        self.server_url = server_url
        self.user = user
        self.password = password
        self.topic_listeners = topic_listeners
        self.logger = logger or logging.root
        self.connection = None
        self.senders = {}

    def on_start(self, event):
        self.connection = event.container.connect(self.server_url, user=self.user, password=self.password)
        for topic in self.topic_listeners.keys():
            self.senders[topic] = event.container.create_sender(self.connection, topic)
            event.container.create_receiver(self.connection, topic)

    def send_message(self, topic, avro_object):
        if not self.connection:
            self.logger.error(f"Connection not established. Cannot create sender for topic: {topic}")
            return

        if topic not in self.senders or self.senders[topic] is None:
            try:
                self.senders[topic] = self.connection.create_sender(topic)
            except Exception as e:
                self.logger.error(f"Failed to create sender for topic '{topic}': {e}")
                return

        serialize = avroer.AvroSerializer(schema=avro_object.schema)
        content = serialize(content=avro_object.data)
        message = Message(body=content, properties={"schema": avro_object.schema.get_prop("name")})
        self.senders[topic].send(message)

    def on_message(self, event):
        topic = event.receiver.source.address
        if topic in self.topic_listeners:
            payload = event.message.body
            deserialize = avroer.AvroDeserializer()
            inferred_schema, data = deserialize(payload=payload)
            for d in data:
                for listener in self.topic_listeners[topic]:
                    schema = listener.schema or inferred_schema
                    avro_object = avroer.AvroObject(schema=schema, data=d)
                    listener.observer.on_message(subject=avro_object)


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


class AvroStomper:
    def __init__(self, host, port, user=None, password=None, auto_reconnect: bool = False, logger=None):
        self.topic_listeners = {}
        server_url = f'amqp://{user}:{password}@{host}:{port}'
        self.logger = logger or logging.root
        self.handler = ProtonHandler(server_url, user, password, self.topic_listeners, logger=logger)
        self.container = Container(self.handler)
        self.thread = threading.Thread(target=self.container.run)
        self.auto_reconnect = auto_reconnect

    def connect(self):
        if not self.thread.is_alive():
            self.thread.start()

    def disconnect(self):
        self.container.stop()
        self.thread.join()

    def send(self, topic: str, avro_object: avroer.AvroObject) -> None:
        if not self.handler.connection:
            self.logger.error("Connection not established. Cannot send message.")
            return
        self.handler.send_message(topic, avro_object)

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema=None,
                  listen_schema_name: str = None) -> None:
        topic_value = TopicValue(topic=topic, listen_schema_name=listen_schema_name, schema=schema, observer=observer)
        if topic not in self.topic_listeners:
            self.topic_listeners[topic] = []
        self.topic_listeners[topic].append(topic_value)
