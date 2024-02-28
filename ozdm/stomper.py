# import asyncio
# from nats.aio.client import Client as NATS
# from concurrent.futures import ThreadPoolExecutor
# import logging
#
# class AvroStomper:
#     def __init__(self, servers, user=None, password=None, logger=None):
#         self.servers = servers
#         self.user = user
#         self.password = password
#         self.logger = logger or logging.getLogger(__name__)
#         self.loop = asyncio.get_event_loop()
#         self.executor = ThreadPoolExecutor(max_workers=4)
#         self.nats = NATS()
#
#     def _run_async(self, coro):
#         """
#         Helper method to run an asynchronous coroutine from synchronous code.
#         """
#         return asyncio.run_coroutine_threadsafe(coro, self.loop).result()
#
#     def connect(self):
#         coro = self.nats.connect(servers=self.servers, user=self.user, password=self.password)
#         self._run_async(coro)
#
#     def disconnect(self):
#         coro = self.nats.close()
#         self._run_async(coro)
#
#     def send(self, subject, message):
#         coro = self.nats.publish(subject, message.encode('utf-8'))
#         self._run_async(coro)
#
#     def subscribe(self, subject, callback):
#         async def async_wrapper(msg):
#             # Run the synchronous callback in the executor to prevent blocking the event loop
#             self.loop.run_in_executor(self.executor, callback, msg)
#
#         coro = self.nats.subscribe(subject, cb=async_wrapper)
#         self._run_async(coro)

import abc
import logging
from typing import Dict, List

import avro.schema
import stomp

from ozdm import avroer


class MessageListener(abc.ABC):

    @abc.abstractmethod
    def on_message(self, subject: avroer.AvroObject) -> None:
        pass


class ReconnectListener(stomp.ConnectionListener):
    _topics: set[str] = set()

    def __init__(self, conn, user=None, password=None, logger=None):
        self.conn = conn
        self.logger = logger or logging.root
        self.user = user
        self.password = password

    def on_error(self, frame):
        self.logger.debug('received an error "%s"' % frame.body)

    def on_message(self, frame):
        self.logger.debug('received a message "%s"' % frame.body)

    def on_disconnected(self):
        self.logger.info('Reconnecting...')
        self.connect()
        i = 1
        for t in self._topics:
            self.conn.subscribe(destination=t, id=i, ack='auto')
            i = i + 1

    def connect(self):
        if self.user is not None and self.password is not None:
            self.conn.connect(self.user, self.password, wait=True)
        else:
            self.conn.connect(wait=True)

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


class TopicListener(stomp.ConnectionListener):
    _observers: Dict[TopicKey, List[TopicValue]] = {}

    def __init__(self, logger=None):
        self.logger = logger or logging.root

    def on_message(self, frame):
        topic = frame.headers["destination"]
        schema_name = frame.headers["schema"]
        observers = []
        k = TopicKey(topic=topic, listen_schema_name=schema_name)
        if k in self._observers.keys():
            for l in self._observers[k]:
                observers.append(l)

        k = TopicKey(topic=topic, listen_schema_name=None)
        if k in self._observers.keys():
            for l in self._observers[k]:
                observers.append(l)

        payload = frame.body
        deserialize = avroer.AvroDeserializer()
        inferred_schema, data = deserialize(payload=payload)
        for d in data:
            for l in observers:
                schema = l.schema or inferred_schema
                avro_object = avroer.AvroObject(schema=schema, data=d)
                l.observer.on_message(subject=avro_object)

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
    _i: int = 1

    def __init__(self, host, port, user=None, password=None, auto_reconnect: bool = False, logger=None):
        self.conn = stomp.Connection(host_and_ports=[(host, port)], heartbeats=(4000, 4000))
        self.logger = logger or logging.root
        self.user = user
        self.password = password
        self.reconnectListener = ReconnectListener(conn=self.conn, user=user, password=password, logger=logger)
        self.topic_listener = TopicListener()
        if auto_reconnect:
            self.conn.set_listener("reconnectListener", self.reconnectListener)
        self.conn.set_listener("topicListener", self.topic_listener)

    def connect(self):
        self.reconnectListener.connect()

    def disconnect(self):
        self.conn.disconnect()

    def subscribe(self, observer: MessageListener, topic: str, schema: avro.schema.Schema=None,
                  listen_schema_name: str = None) -> None:
        self.topic_listener.subscribe(observer=observer, topic=topic, schema=schema,
                                      listen_schema_name=listen_schema_name)
        self.conn.subscribe(topic, id=topic)
        self.reconnectListener.subscribed(topic=topic)

    def send(self, topic: str, avro_object: avroer.AvroObject) -> None:
        serialize = avroer.AvroSerializer(schema=avro_object.schema)
        content = serialize(content=avro_object.data)
        self.conn.send(destination=topic, body=content, headers={"schema": avro_object.schema.get_prop("name")})
