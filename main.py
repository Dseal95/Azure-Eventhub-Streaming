

from src.EventStream import EventHubStreamer


if "__main__" == __name__:

    streamer = EventHubStreamer()

    streamer.stream_events()

    streamer.df 