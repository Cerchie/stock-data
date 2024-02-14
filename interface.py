import pandas as pd
import streamlit as st
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from setupsocket import on_select
import numpy as np
import json

config_parser = ConfigParser(interpolation=None)
config_file = open("config.properties", "r")
config_parser.read_file(config_file)
client_config = dict(config_parser["kafka_client"])

consumer = Consumer(client_config)


option = st.selectbox(
    "Which stock would you like to see data for?",
    ("AAPL", "BABA", "NVDA", "MSFT"),
    index=None,
)

st.write("You selected:", option)

# bar chart will display last several stock averages

chart_data = pd.DataFrame(np.random.randn(3), columns=["a"])

st.bar_chart(chart_data)

# if isinstance(option, str):


class Stock(object):
    """
    Stock record

    Args:
        price (int): Stock price
        window_start (str): Start of avg'd window
        window_end (str): End of avg'd window
    """

    def __init__(self, price=None, window_start=None, window_end=None):
        self.price = price
        self.window_start = window_start
        self.window_end = window_end


def dict_to_stock(obj, ctx):
    """
    Converts object literal(dict) to a Stock instance.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        obj (dict): Object literal(dict)
    """

    if obj is None:
        return None

    return Stock(
        price=obj["price"],
        window_start=obj["window_start"],
        window_end=obj["window_end"],
    )


schema_str = """{
  "properties": {
    "price": {
      "connect.index": 2,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "connect.type": "float64",
          "type": "number"
        }
      ]
    },
    "window_end": {
      "connect.index": 1,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "window_start": {
      "connect.index": 0,
      "oneOf": [
        {
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    }
  },
  "title": "Record",
  "type": "object"
}"""

json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_stock)


placeholder = st.empty()

data = []


# on_select(option)

consumer.subscribe(["tumble_interval"])

msg = consumer.poll()

while True:
    try:
        msg = consumer.poll()
        if msg is None:
            continue

        stock = json_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )

        if stock is not None:
            print(
                "Stock record {}: price: {}"
                "window_start:{}"
                "window_end: {}".format(
                    msg.key(), stock.price, stock.window_start, stock.window_end
                )
            )
    except KeyboardInterrupt:
        break

consumer.close()

with placeholder:
    data.append(msg.value())
    st.write(pd.DataFrame(data))

# It is important to exit the context of the placeholder in each step of the loop
# placeholder object should have the same methods for displaying data as st
# placeholder.dataframe(df)
