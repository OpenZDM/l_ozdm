import abc
import logging
from typing import Dict, List

import avro.schema
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

from ozdm import avroer
from ozdm.avroer import AvroSerializer, AvroDeserializer

class MessageListener(abc.ABC):

    @abc.abstractmethod
    def on_message(self, subject: avroer.AvroObject) -> None:
        pass

class ReconnectListener(MessagingHandler):
    _topics: set[str] = set()

    def __init__(self, user=None, password=None, logger=None):
        super().__init__()
        self.logger = logger or logging.root
        self.user = user
        self.password = password
        self.connection = None

    def on_error(self, event):
        self.logger.debug('received an error "%s"' % event.message.body)

    def on_message(self, event):
        self.logger.debug('received a message "%s"' % event.message.body)

    def connect(self):  # Keep the 'connect' method here as requested.
        conn_url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"
        self.container.create_connection(conn_url)
        self.container.run()
        self.connection = self.container

    def on_disconnected(self, event):
        self.logger.info('Reconnecting...')
        self.connect()
        i = 1
        for t in self._topics:
            self.connection.create_receiver(f"{t}:{i}")
            i = i + 1

    def on_start(self, event):
        if self.user is not None and self.password is not None:
            event.container.sasl_enabled = True
            event.container.allowed_mechs = "PLAIN"
        event.container.connect(self.user, self.password)
        self.connection = event.container

    def on_subscribe(self, event):
        i = 1
        for t in self._topics:
            event.container.create_receiver(event.connection, t, name=i)
            i = i + 1

    def subscribed(self, topic):
        self._topics.add(topic)

class TopicValue:
    def __init__(self, topic: str, listen_schema_name: str, schema: avro.schema.Schema, observer: MessageListener):
        self.topic = topic
        self.listen_schema_name = listen_schema_name
        self.schema = schema
        self.observer = observer

    def __eq__(self, other):
        """Overrides the default implementation"""
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
                print(f"Received message: {avro_object}")

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema,
                  listen_schema_name: str = None) -> None:
        k = TopicKey(topic=topic, listen_schema_name=listen_schema_name)
        if k not in self._observers.keys():
            self._observers[k] = []
        self._observers[k].append(TopicValue(topic=topic,
                                             listen_schema_name=listen_schema_name,
                                             schema=schema,
                                             observer=observer))

class AvroStomper:
    def __init__(self, host, port, user=None, password=None, logger=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.logger = logger or logging.root
        self.container = Container(ReconnectListener(user=user, password=password, logger=logger))
        self.topic_listener = TopicListener(logger=logger)
        self.connection = None

    def connect(self):
        conn_url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"
        self.container.create_sender(conn_url)
        self.container.create_receiver(conn_url)
        self.container.run()
        self.connection = self.container

    def disconnect(self):
        self.container.stop()

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema = None,
                  listen_schema_name: str = None) -> None:
        self.topic_listener.subscribe(observer=observer, topic=topic, schema=schema,
                                      listen_schema_name=listen_schema_name)
        ReconnectListener.subscribed(self, topic)

    def send(self, topic: str, avro_object: avroer.AvroObject) -> None:
        serialize = avroer.AvroSerializer(schema=avro_object.schema)
        content = serialize(content=avro_object.data)

        message = Message()
        message.subject = topic
        message.body = content
        message.properties = {"schema": avro_object.schema.get_prop("name")}

        if self.connection:
            sender = self.connection.create_sender(topic)
            sender.send(message)
        else:
            self.logger.error("Connection not established.")
