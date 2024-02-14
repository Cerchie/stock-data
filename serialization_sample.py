# SAMPLE I'M WORKING FROM

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from configparser import ConfigParser


if __name__ == "__main__":
    topic = "test-topic"

    # Configure Kafka consumer
config_parser = ConfigParser(interpolation=None)
config_file = open("config.properties", "r")
config_parser.read_file(config_file)
client_config = dict(config_parser["kafka_client"])

consumer = Consumer(client_config)


# Configure SR client
sr_client = SchemaRegistryClient(
    {
        "url": "http://localhost:8081",
    }
)
schema_str = """{
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://example.com/myURI.schema.json",
    "title": "SampleRecord",
    "description": "Sample schema to help you get started.",
    "type": "object",
    "additionalProperties": false,
    "properties": {
        "myField1": {
        "type": "integer",
        "description": "The integer type is used for integral numbers."
        },
        "myField2": {
        "type": "number",
        "description": "The number type is used for any numeric type, either integers or floating point numbers."
        },
        "myField3": {
        "type": "string",
        "description": "The string type is used for strings of text."
        }
    }
    }"""
json_deserializer = JSONDeserializer(
    schema_str,
    schema_registry_client=sr_client,
)

try:
    consumer.subscribe("tumble_interval")
    while True:
        try:
            msg = consumer.poll(timeout=0.25)
            if msg is not None:
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    message_deserialised = json_deserializer(
                        msg.value(),
                        SerializationContext(
                            msg.topic(),
                            MessageField.VALUE,
                        ),
                    )
                    print(f"Headers: {msg.headers()}")
                    try:
                        print(
                            f"Key: {None if msg.key() is None else msg.key().decode()}"
                        )
                    except Exception:
                        print(msg.key())
                    print(f"Value: {message_deserialised}\n")

        except Exception as err:
            print(msg.key(), msg.value())
            print(err)

except KeyboardInterrupt:
    print("CTRL-C pressed by user!")
    pass

finally:
    consumer.close()
