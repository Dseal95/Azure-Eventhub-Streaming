"""Module containing functionality to stream events from Azure Eventhub."""

from datetime import timedelta, datetime
import logging
import threading
import time

import pandas as pd
from azure.eventhub import EventHubConsumerClient


logging.getLogger().setLevel(logging.INFO)


class AzureEventHubStreamer:
    """Class to stream events from Azure Eventhub."""

    def __init__(self, config):
        """Initialisation."""
        self.eventhub_name = config["eventhub_name"]
        self.connection_string = config["connection_string"]
        self.consumer_group = config["consumer_group"]
        self.streaming_duration = config["streaming_duration"]
        self.streaming_window = config["streaming_window"]

        # list to store events
        self.streamed_events = []

        # event df
        self.df = None

    def stream_events(self):
        """Stream events from Azure Eventhub."""
        # set up eventhub connection (client)
        consumer_client = self.set_up_eventhub_connection(
            connection_string=self.connection_string,
            consumer_group=self.consumer_group,
            eventhub_name=self.eventhub_name,
        )

        # calculate the streaming starting position
        starting_position = datetime.now() - timedelta(
            hours=0, minutes=self.streaming_window
        )

        # stream events from EventHub
        thread = threading.Thread(
            target=consumer_client.receive,
            kwargs={
                "on_event": self.on_event,
                "starting_position": starting_position,  # datetime value
                "starting_position_inclusive": True,
                "on_partition_initialize": self.on_partition_initialize,
                "on_partition_close": self.on_partition_close,
                "on_error": self.on_error,
            },
        )
        thread.daemon = True
        # start the thread
        thread.start()
        # only stream for receive_duration seconds
        time.sleep(self.streaming_duration)
        # close the connection to EventHub
        consumer_client.close()
        # terminate the thread
        thread.join()

        logging.info(
            f'\t<stream_events()> Finished streaming events @ {starting_position.strftime("%Y-%m-%d %H:%M:%S")}'
        )

        # store the events streamed from EventHub in a Pandas DataFrame
        event_data = self.serialise_list(list_series=self.streamed_events)

        if event_data is not None:
            self.df = event_data.apply(pd.Series)

        return self

    def on_event(self, partition_context, event):
        logging.info(
            "\t<on_event()> Received event from partition {}".format(
                partition_context.partition_id
            )
        )

        # update checkpoint to stream from
        partition_context.update_checkpoint(event)

        # add events to list
        self.streamed_events.append(event.body_as_json())

        return self

    def on_partition_initialize(self, partition_context):
        logging.info(
            "\t<on_partition_initialize()> Partition: {} has been initialized.".format(
                partition_context.partition_id
            )
        )

        return self

    def on_partition_close(self, partition_context, reason):
        logging.info(
            "\t<on_partition_close()> Partition: {} has been closed, reason for closing: {}.".format(
                partition_context.partition_id, reason
            )
        )

        return self

    def on_error(self, partition_context, error):
        if partition_context:
            logging.info(
                "\t<on_error()> An exception: {} occurred during receiving from Partition: {}.".format(
                    partition_context.partition_id, error
                )
            )
        else:
            logging.info(
                "\t<on_error()> An exception: {} occurred during the load balance process.".format(
                    error
                )
            )

        return self

    @staticmethod
    def serialise_list(list_series: list):
        """Serialise a list of dictionaries.

        Args:
            list_series: Input.

        Returns:
            pd.Series: a Series of the original list of dictionaries."""
        if len(list_series) == 0:
            return None
        else:
            series = pd.Series(list_series)

        return series

    @staticmethod
    def set_up_eventhub_connection(
        connection_string: str, consumer_group: str, eventhub_name: str
    ):
        """Set up connection to Azure EventHub."""
        logging.info(
            f"\t<set_up_eventhub_connection()> Connecting to Azure EventHub: {eventhub_name}"
        )
        consumer_client = EventHubConsumerClient.from_connection_string(
            conn_str=connection_string,
            consumer_group=consumer_group,
            eventhub_name=eventhub_name,
        )

        return consumer_client
