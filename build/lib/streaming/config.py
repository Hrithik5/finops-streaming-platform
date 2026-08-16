"""
Runtime configuration for Databricks jobs.
"""

CONFLUENT_BOOTSTRAP_SERVERS = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"

CONFLUENT_SECRET_SCOPE = "confluent-kafka"


def get_kafka_options(dbutils) -> dict[str, str]:
    """Build Kafka connection options from Databricks secrets."""

    api_key = dbutils.secrets.get(
        scope=CONFLUENT_SECRET_SCOPE,
        key="api-key",
    )

    api_secret = dbutils.secrets.get(
        scope=CONFLUENT_SECRET_SCOPE,
        key="api-secret",
    )

    return {
        "kafka.bootstrap.servers": CONFLUENT_BOOTSTRAP_SERVERS,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain."
            "PlainLoginModule "
            f'required username="{api_key}" '
            f'password="{api_secret}";'
        ),
    }
