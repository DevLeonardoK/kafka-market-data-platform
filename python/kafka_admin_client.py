from kafka.admin import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

kafka_admin_client = KafkaAdminClient(
    bootstrap_servers=[
        "kafka_broker_1:5091",
        "kafka_broker_2:5092",
        "kafka_broker_3:5093"
    ],
    client_id="kafka_admin_client"
)

market_data_raw_topic = "market_data_raw_topic"

try:
    kafka_admin_client.create_topics(
        {
            "market_data_raw_topic": {
                "num_partitions": 9,
                "replication_factor": 1
            }
        },
        validate_only=False
    )

except TopicAlreadyExistsError:
    pass
finally:
    kafka_admin_client.close()
