from src.EventStream import AzureEventHubStreamer
from src.utils import Config

if "__main__" == __name__:
    # read in config
    config = Config("config.yaml")._params

    # instantiate the streamer class
    streamer = AzureEventHubStreamer(config=config)

    # stream events
    streamer.stream_events()

    # TO DO: placeholder for code to handle event df
    # df = streamer.df
