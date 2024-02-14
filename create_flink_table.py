from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col


import json

import config

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table_env.get_config().set(
    "pipeline.jars",
    "file:////Users/lcerchie/Downloads/flink-sql-connector-kafka-3.0.2-1.18.jar",
)

sink_ddl = """
CREATE TABLE print_table WITH (
        'connector' = 'print'
    ) LIKE tumble_interval (EXCLUDING ALL)
    """

source_ddl = f"""
    CREATE TABLE tumble_interval (
        window_start STRING,
        window_end STRING,
        price DOUBLE
    ) WITH (
   'connector' = 'kafka',
   'properties.security.protocol' = 'SASL_SSL',
   'properties.sasl.mechanism' = 'PLAIN',
   'topic' = 'tumble_interval', 
   'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.CC_KEY}" password="{config.CC_SECRET}";',
   'properties.bootstrap.servers' = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
   'properties.group.id' = 'stocks_consumer',
   'scan.startup.mode' = 'earliest-offset',
   'format' = 'confluent-avro'
    )
"""

source_table = table_env.execute_sql(source_ddl).wait()

sink_table = table_env.execute_sql(sink_ddl)

table_env.execute_sql("INSERT INTO print_table SELECT * FROM tumble_interval").wait()
