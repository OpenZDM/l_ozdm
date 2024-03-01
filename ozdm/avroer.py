import io
import json
import avro.schema
import avro.datafile
import requests


class AvroObject:
    def __init__(self, schema: avro.schema.Schema, data: dict):
        self.schema = schema
        self.data = data

    def entity(self, o=None):
        if o is None:
            class_name = self.schema.get_prop("name")
            o = eval(f"{class_name}()")
        for key, value in self.data.items():
            setattr(o, key, value)
        return o

    @property
    def json_schema(self) -> object:
        return self.schema.to_json()


class AvroSchema:
    def __init__(self, schema_type="record", schema_namespace: str = None):
        self.schema_namespace = schema_namespace
        self.schema_type = schema_type

    def schema(self, schema_name: str = None, attrs: dict = None, obj: object = None, data_types: dict = None) -> avro.schema.Schema:
        if obj is None and attrs is None:
            raise ValueError("attrs or obj must be specified!")

        if obj is None and schema_name is None:
            raise ValueError("schema_name must be specified!")

        schema_name = schema_name or obj.__class__.__name__

        if attrs is None:
            attrs = obj.__dict__

        data_types = data_types or {}
        schema_dict = {
            "type": self.schema_type,
            "name": schema_name,
            "namespace": self.schema_namespace,
            "fields": []
        }
        for k in attrs.keys():
            value = attrs[k] if k in attrs.keys() else None
            data_type = data_types[k] if k in data_types.keys() else type(attrs[k])
            avro_type = AvroSchema.get_type(value=value, data_type=data_type)

            schema_dict["fields"].append({"name": k, "type": avro_type})
        return avro.schema.parse(json.dumps(schema_dict))

    @staticmethod
    def get_type(value: object, data_type: type = None):
        if value is None and data_type is None:
            raise TypeError("value is None but the type is not explicit in data_types")

        if data_type is None:
            dt = type(value)
        else:
            dt = data_type

        if dt == str:
            dt_type = "string"
        elif dt == int:
            dt_type = "int"
        elif dt == float:
            dt_type = "float"
        elif dt == bool:
            dt_type = "boolean"
        elif dt == list:
            if value is None or len(value) == 0:
                dt_type = {"type": "array", "items": "string", "default": []}
            else:
                dt_type = {"type": "array", "items": AvroSchema.get_type(value[0]), "default": []}
        elif dt == dict:
            if value is None or len(value) == 0:
                dt_type = {"type": "map", "values": "string", "default": {}}
            else:
                dt_type = {"type": "map", "values": AvroSchema.get_type(list(value.values())[0]), "default": {}}
        else:
            raise TypeError(f"{type(value)} unknown")

        if value is None:
            return ['null', dt_type]
        else:
            return dt_type


class AvroSerializer:
    def __init__(self, schema: avro.schema.Schema):
        self.avro_schema = schema

    def __call__(self, content: dict) -> bytes:
        buf = io.BytesIO()
        writer = avro.datafile.DataFileWriter(buf, avro.io.DatumWriter(), self.avro_schema)
        writer.append(content)
        writer.flush()
        buf.seek(0)
        data = buf.read()
        return data


class AvroDeserializer:
    def __call__(self, payload: bytes) -> (avro.schema.Schema, list):
        message_buf = io.BytesIO(payload)
        reader = avro.datafile.DataFileReader(message_buf, avro.io.DatumReader())
        schema = reader.schema
        content = []
        for thing in reader:
            content.append(thing)
        reader.close()
        return schema, content


class SchemaException(Exception):
    def __init__(self, message):
        super().__init__(message)


class SchemaManager:
    def __init__(self, registry: str):
        self.registry = registry

    def get_schema(self, group_id: str, schema_id: str) -> avro.schema.Schema:
        try:
            schema = requests.get(f"{self.registry.rstrip('/')}/{group_id}/artifacts/{schema_id}")
            if schema.status_code == 200:
                return avro.schema.parse(json_string=json.dumps(schema.json()))
            else:
                raise SchemaException(f"{group_id}/{schema_id} does not exist in the registry {self.registry}")
        except Exception as ex:
            print(ex)
            raise SchemaException(f"Failed to retrieve schema {group_id}/{schema_id} from the registry {self.registry}: {ex}")

    def put_schema(self, group_id: str, schema_id: str, schema: avro.schema.Schema):
        response = requests.post(url=f"{self.registry.rstrip('/')}/{group_id}/artifacts", json=schema.to_json(), headers={
            "Content-Type": "application/json",
            "X-Registry-ArtifactId": schema_id,
            "X-Registry-ArtifactType": "AVRO"
        })
        if response.status_code not in [200, 201]:
            raise SchemaException(f"Error registering schema {schema_id} with registry {self.registry}: {response.text}")
