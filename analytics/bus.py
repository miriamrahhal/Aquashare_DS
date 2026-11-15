"""RabbitMQ names shared by the event relay and the analytics service."""
import pika

EXCHANGE = "aquashare.events"      # topic exchange; the routing key is the event type
QUEUE = "analytics.totals"         # durable queue read by the analytics service


def declare(channel) -> None:
    """Declare the exchange and the analytics queue. Safe to call any number of times.

    Both the relay and the analytics service declare them, so events published before
    the analytics service has ever started are kept in the queue instead of being dropped."""
    channel.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(QUEUE, durable=True)
    channel.queue_bind(QUEUE, EXCHANGE, routing_key="#")


def connect(url: str):
    connection = pika.BlockingConnection(pika.URLParameters(url))
    channel = connection.channel()
    declare(channel)
    return connection, channel
