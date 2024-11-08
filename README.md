# Consumer Group Monitoring
This script monitors the state of consumer groups using the Confluent Cloud API.

## Installation
Clone the repository and navigate to the project directory.

Rename the .env.example file to .env and fill in the following values:
```
HEADER: Base64 encode your api_key:secret. Ensure the service account that owns this key has the necessary access permissions for the cluster.
CLUSTER_ID: Your Confluent Cloud cluster ID.
ENDPOINT: The API endpoint for your Confluent Cloud instance.
```

Install the required Python packages:

```
pip install -r requirements.txt
```

## Usage
Configure the runtime duration by modifying the MINUTES variable.

Adjust the level of parallelism by modifying the THREADS variable. The number of consumer groups will be distributed evenly among the threads.

## Run the script:
```
python monitor_consumer_groups.py
```