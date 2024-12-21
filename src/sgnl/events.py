"""a module for storing event processing utilities"""

# Copyright (C) Patrick Godwin
# Copyright (C) 2024 Yun-Jing Huang
__author__ = (
    "Patrick Godwin (patrick.godwin@ligo.org), Yun-Jing Huang (yun-jing.huang@ligo.org)"
)

# -------------------------------------------------
# imports

import json
import logging
import signal
import sys
import time
import timeit

from ligo.scald.utils import gps_now

try:
    from confluent_kafka import Consumer, KafkaError, Producer
    from confluent_kafka.admin import AdminClient, NewTopic
except ImportError:
    raise ImportError("confluent_kafka is required for this module")


# -------------------------------------------------
# classes


class EventProcessor(object):
    """Base class for processing events via Kafka.

    Args:
        kafka_server:
            str, the host:port combination to connect to the Kafka broker
        input_topic:
            str, the name of the input topic(s)
        output_topic:
            str, the name of the output topic(s)
        topic_partitions:
            int, number of partitions to create new output topics with, default is 1
        process_cadence:
            float, maximum rate at which data is processed, defaults to 0.1s
        request_timeout:
            float, timeout for requesting messages from a topic, defaults to 0.2s
        num_messages:
            int, max number of messages to process per cadence, defaults to 10
        tag:
            str, a nickname for the instance, defaults to 'default'
        send_heartbeats:
            bool, send periodic heartbeat messages to Kafka for monitoring
        heartbeat_cadence:
            float, cadence on which to write heartbeat messages to Kafka
        heartbeat_topic:
            str, Kafka topic to send heartbeats to
    """

    _name = "processor"

    def __init__(
        self,
        process_cadence=0.1,
        request_timeout=0.2,
        num_messages=10,
        kafka_server=None,
        input_topic=None,
        output_topic=None,
        topic_partitions=None,
        tag="default",
        send_heartbeats=False,
        heartbeat_cadence=None,
        heartbeat_topic=None,
    ):
        # set up input options
        assert kafka_server, "kafka_server needs to be set"
        self.is_source = not bool(input_topic)
        self.is_sink = not bool(output_topic)
        if isinstance(input_topic, str):
            input_topic = [input_topic]
        if self.is_sink:
            assert topic_partitions is None
        else:
            if isinstance(output_topic, str):
                output_topic = [output_topic]
            if not topic_partitions:
                topic_partitions = [1] * len(output_topic)
            elif isinstance(topic_partitions, int):
                topic_partitions = [topic_partitions]
            assert len(output_topic) == len(topic_partitions)

        # processing settings
        self.process_cadence = process_cadence
        self.request_timeout = request_timeout
        self.num_messages = num_messages
        self.is_running = False

        # kafka settings
        self.tag = tag
        self.kafka_settings = {
            "bootstrap.servers": kafka_server,
        }
        self.producer_settings = {
            "message.max.bytes": 10485760,  # 10 MB
            **self.kafka_settings,
        }
        self.producer = Producer(self.producer_settings)
        if not self.is_source:
            self.kafka_settings["group.id"] = "-".join([self._name, tag])
            self.consumer = Consumer(self.kafka_settings)
            self.consumer.subscribe([topic for topic in input_topic])

        # init output topics
        self.admin_client = AdminClient({"bootstrap.servers": kafka_server})
        if not self.is_sink:
            for topic_name, partitions in zip(output_topic, topic_partitions):
                if not self.topic_exists(topic_name):
                    # create the new topic with
                    # specified number of partitions
                    new_topic = NewTopic(topic=topic_name, num_partitions=partitions)
                    self.admin_client.create_topics([new_topic])
                else:
                    pass

        # signal handler
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, self.catch)

        # heartbeat functions for monitoring
        self.send_heartbeats = send_heartbeats
        if self.send_heartbeats:
            self.heartbeat_topic = heartbeat_topic
            self.last_heartbeat = float(gps_now())
            self.heartbeat_cadence = heartbeat_cadence

    def fetch(self):
        """Fetch for messages from a topic and processes them."""
        logging.debug("polling for new events")
        messages = self.consumer.consume(
            num_messages=self.num_messages, timeout=self.request_timeout
        )

        for message in messages:
            # only add to queue if no errors in receiving data
            if message:
                if not message.error():
                    self.ingest(message)
                elif not message.error().code() == KafkaError._PARTITION_EOF:
                    logging.warning("Received message with error: %s", message.error())

    def process(self):
        """Processes events at the specified cadence."""
        while self.is_running:
            start = timeit.default_timer()
            if not self.is_source:
                self.fetch()
            self.handle()
            if self.send_heartbeats:
                self.heartbeat()
            elapsed = timeit.default_timer() - start
            time.sleep(max(self.process_cadence - elapsed, 0))

    def start(self):
        """Starts the event loop."""
        logging.info("starting %s...", self._name.replace("_", " "))
        self.is_running = True
        self.process()

    def stop(self):
        """Stops the event loop."""
        logging.info("shutting down %s...", self._name.replace("_", " "))
        self.finish()
        self.is_running = False
        self.producer.flush()

    def catch(self, signum, frame):
        """Shuts down the event processor gracefully before exiting."""
        logging.info("SIG %d received, attempting graceful shutdown...", signum)
        self.stop()
        sys.exit(0)

    def ingest(self, message):
        """Ingests a single event.

        NOTE: Derived classes need to implement this.
        """
        return NotImplementedError

    def handle(self):
        """Handles ingested events.

        NOTE: Derived classes need to implement this.
        """
        return NotImplementedError

    def finish(self):
        """Finish remaining events when stopped and/or shutting down.

        NOTE: Derived classes may implement this if desired.
        """
        pass

    def heartbeat(self):
        """Send heartbeat messages to Kakfa to monitor
        the health of this process.
        """
        time_now = float(gps_now())

        if time_now - self.last_heartbeat >= self.heartbeat_cadence:
            self.last_heartbeat = time_now
            self.producer.produce(
                topic=self.heartbeat_topic,
                value=json.dumps(
                    {
                        "time": [time_now],
                        "data": [1],
                    }
                ),
                key=self.tag,
            )
            self.producer.poll(0)

    def topic_exists(self, topic_name):
        topic_metadata = self.admin_client.list_topics()
        return topic_name in topic_metadata.topics


# -------------------------------------------------
# utilities


def append_args(parser):
    """Append event processing specific options to an ArgumentParser instance."""
    group = parser.add_argument_group("Event processing options")
    group.add_argument(
        "--tag",
        metavar="string",
        default="default",
        help="Sets the name of the tag used. Default = 'default'",
    )
    group.add_argument(
        "--processing-cadence",
        type=float,
        default=0.1,
        help="Rate at which the event uploader acquires and processes data. Default = "
        "0.1 seconds.",
    )
    group.add_argument(
        "--request-timeout",
        type=float,
        default=0.2,
        help="Timeout for requesting messages from a topic. Default = 0.2 seconds.",
    )
    group.add_argument(
        "--kafka-server",
        metavar="string",
        help="Sets the server url that the kafka topic is hosted on. Required.",
    )
    group.add_argument(
        "--input-topic",
        metavar="string",
        action="append",
        help="Sets the input kafka topic. Required.",
    )

    return parser
