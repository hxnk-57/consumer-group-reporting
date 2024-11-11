# Consumer Group Monitoring
This script monitors the state of consumer groups using the Confluent Cloud API.

## Installation
Clone the repository and navigate to the project directory.

Rename the .env.example file to .env and fill in the following values:
```
HEADER: ""
CLUSTER_ID: ""
ENDPOINT: ""
CLUSTER_ALIAS: ""
```

- **HEADER**: Base64 encode your `api_key:secret`. Ensure the service account that owns this key has the necessary access permissions for the cluster.
- **CLUSTER_ID**: Your Confluent Cloud cluster ID.
- **ENDPOINT**: The API endpoint for your Confluent Cloud instance.
- **CLUSTER_ALIAS**: An optional alias for your cluster for display purposes.

Next, create a virtual environment:
```
python -m venv <virtual-environment-name>
```

Activate the virtual environment:
```
source virtual-environment-name/bin/activate
```

Install the required Python packages:
```
pip install -r requirements.txt
```

## Usage
- Configure the runtime duration by modifying the `MINUTES` variable.
- Adjust the level of parallelism by modifying the `THREADS` variable. The number of consumer groups will be distributed evenly among the threads.
- Configure the name of the report by modifying the `REPORT_NAME` variable.


## Run the script:
The script polls the Confluent API continuosly. Start polling with:
```
python monitor_consumer_groups.py
```

## Dashboard
The dashboard provides view of the consumer group states during or after polling.
To monitor the state changes start the streamlit app:
```
streamlit run dashboard.py
```