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

class ReconnectListener(MessagingHandler):
    def __init__(self, container, host, port, user, password, logger, connection_callback):
        super().__init__()
        self.container = container
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.logger = logger or logging.root
        self.connection = None
        self.connection_callback = connection_callback

    def on_error(self, event):
        self.logger.error('Received an error: %s' % event.message.body)

    def on_message(self, event):
        self.logger.info('Received a message: %s' % event.message.body)

    def connect(self):
        if not self.connection:
            try:
                self.logger.info("Attempting to connect...")
                conn_url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"
                self.connection = self.container.connect(conn_url, reconnect=True)
                self.logger.info(f"Connecting to {conn_url}")
            except Exception as e:
                self.logger.error(f"Error in connection: {e}")
                self.connection = None

    def on_start(self, event):
        if self.user and self.password:
            event.container.sasl_enabled = True
            event.container.allowed_mechs = "PLAIN"
        self.connect()

    def on_connection_opened(self, event):
        self.logger.info("Connection opened")
        self.connection_callback(event.connection)

    def on_disconnected(self, event):
        self.logger.info("Disconnected")
        self.connection_callback(None)


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

class AvroStomper:
    def __init__(self, host, port, user, password, logger=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self.container = Container()
        self.reconnect_listener = ReconnectListener(
            container=self.container,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            logger=self.logger,
            connection_callback=self.update_connection
        )
        self.container.selectable(self.reconnect_listener)
        self.connection = None
        self.topic_listener = TopicListener(logger=self.logger)

    def update_connection(self, connection):
        self.connection = connection
        if connection:
            self.logger.info("Connection established.")
        else:
            self.logger.warning("Connection lost.")

    def connect(self):
        if not self.connection:
            self.logger.info("Initializing AvroStomper connection...")
            try:
                self.container.run()
                self.logger.info("AvroStomper connected successfully.")
            except Exception as e:
                self.logger.error(f"Error in AvroStomper connect: {e}")

    def disconnect(self):
        self.container.stop()

    def send(self, topic, avro_object):
        if not self.connection:
            self.logger.error("Connection not established.")
            return
        try:
            serialize = AvroSerializer(schema=avro_object.schema)
            content = serialize(content=avro_object.data)
            message = Message()
            message.subject = topic
            message.body = content
            sender = self.connection.create_sender(topic)
            sender.send(message)
            self.logger.info(f"Message sent to {topic}")
        except Exception as e:
            self.logger.error(f"Error sending message: {e}")

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema = None,
                  listen_schema_name: str = None) -> None:
        self.topic_listener.subscribe(observer=observer, topic=topic, schema=schema,
                                      listen_schema_name=listen_schema_name)
        self.reconnect_listener.subscribed(topic)
