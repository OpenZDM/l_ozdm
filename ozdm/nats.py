import asyncio
import logging
from typing import Dict, List, Callable
import avro.schema
from nats.aio.client import Client as NATS
from ozdm import avroer

class NatsManager:
    def __init__(self, servers: str, user: str = None, password: str = None, logger: logging.Logger = None):
        self.servers = servers
        self.user = user
        self.password = password
        self.logger = logger or logging.getLogger(__name__)
        self.nats = NATS()
        self.message_handlers: Dict[str, Callable[[str, avroer.AvroObject], None]] = {}

    async def connect(self):
        await self.nats.connect(servers=self.servers, user=self.user, password=self.password)
        self.logger.info("Connected to NATS")

    async def disconnect(self):
        await self.nats.close()
        self.logger.info("Disconnected from NATS")

    async def publish(self, subject: str, avro_object: avroer.AvroObject):
        serializer = avroer.AvroSerializer(schema=avro_object.schema)
        message = serializer(avro_object.data)
        await self.nats.publish(subject, message)
        self.logger.info(f"Published message to {subject}")

    def subscribe(self, subject: str, message_handler: Callable[[str, avroer.AvroObject], None]):
        self.message_handlers[subject] = message_handler

        async def nats_message_handler(msg):
            data = msg.data
            deserializer = avroer.AvroDeserializer()
            schema, decoded_data = deserializer(data)
            avro_obj = avroer.AvroObject(schema=schema, data=decoded_data[0])
            if subject in self.message_handlers:
                await self.message_handlers[subject](msg.subject, avro_obj)

        asyncio.create_task(self.nats.subscribe(subject, cb=nats_message_handler))
        self.logger.info(f"Subscribed to {subject} with handler")
