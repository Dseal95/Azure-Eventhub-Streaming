
# Azure Eventhub Streaming
A standalone class `AzureEventHubStreamer` that is able to stream events from Azure eventhub and store them in a Pandas dataframe. Class is config driven and requires that you instantiate it with key eventhub information: 
- **connection string**
- **eventhub name**
- **consumer group**

**NOTE:** Inspiration taken from https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/eventhub/azure-eventhub/samples/sync_samples/recv.py. I modified the code with main changes being that I added a way of storing the events. 

### Setting Up

- (1) Clone the repository, https://github.com/Dseal95/Azure-Eventhub-Streaming.git
- (2) (Optional) Set up a virtual environment by running the commands in `setup_venv.sh` (ensure you are using desired python alias):

```
python -m venv .venv
source .venv/bin/activate
```

### Workflow
- Workflow outlined in `main.py`:
    - Read in config using `Config()` class from `utils.py`
    - Instantiate  `AzureEventHubStreamer()`
    - Call `@classmethod` `stream_events()` to stream events
    - Inspect events by accessing `.df` class attribute


