import abc
import logging
import threading
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
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self.container = None
        self.connection = None
        self.sender = None
        self.topic = None
        self.is_connected = False

    def on_start(self, event):
        self.connect()

    def connect(self, topic=None):
        self.topic = topic
        if not self.is_connected:
            self.container = Container(self)
            thread = threading.Thread(target=self.run_container)
            thread.daemon = True
            thread.start()
            self.logger.info(f"Attempting to connect to {self.host}:{self.port}")

    def run_container(self):
        try:
            conn_url = f"amqp://{self.user}:{self.password}@{self.host}:{self.port}"
            self.connection = self.container.connect(conn_url)
            self.is_connected = True
            self.container.run()
        except Exception as e:
            self.logger.error(f"Error in connection: {e}", exc_info=True)
            self.is_connected = False

    def on_connection_opened(self, event):
        self.logger.info("Connection opened")
        self.is_connected = True

    def on_disconnected(self, event):
        self.logger.info("Disconnected")
        self.is_connected = False

    def send(self, avro_object):
        if not self.is_connected:
            self.logger.error("Connection not established. Attempting to reconnect...")
            self.connect(topic=self.topic)
            if not self.is_connected:
                self.logger.error("Failed to establish connection. Cannot send message.")
                return
        try:
            serialize = AvroSerializer(schema=avro_object.schema)
            content = serialize(content=avro_object.data)
            message = Message()
            message.subject = self.topic
            message.body = content
            if not self.sender:
                self.sender = self.connection.create_sender(self.topic)
            self.sender.send(message)
            self.logger.info(f"Message sent to {self.topic}")
        except Exception as e:
            self.logger.error(f"Error while sending message: {e}", exc_info=True)
