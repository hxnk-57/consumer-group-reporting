from datetime import datetime
from dotenv import load_dotenv
from typing import List
import concurrent.futures
import csv
import http.client
import json
import logging
import numpy as np
import os
import threading
import time

load_dotenv()

HEADERS = { 'Authorization': f"Basic {os.getenv('HEADER')}" }
CONSUMER_GROUP_ID = ""
CLUSTER_ID = os.getenv("CLUSTER_ID")
ENDPOINT = os.getenv("ENDPOINT")
THREADS = 10
MINUTES = 30

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if not all([HEADERS['Authorization'], CLUSTER_ID, ENDPOINT]):
    logging.error("Missing required environment variables.")
    exit(1)

file_lock = threading.Lock()
consumer_groups_states_lock = threading.Lock()

with open("consumer_group_states.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["timestamp", "consumer_group_id", "state"])


def make_request(conn: http.client.HTTPSConnection, method: str, endpoint: str) -> dict:
    try:
        conn.request(method, endpoint, headers=HEADERS)
        response = conn.getresponse()
        data_str = response.read().decode("utf-8")
        return json.loads(data_str)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {endpoint}: {e}")
    except Exception as e:
        logging.error(f"Error making request to {endpoint}: {e}")
    return {}


def get_consumer_group_ids(conn: http.client.HTTPSConnection) -> List[str]:
    logging.info(f"Fetching consumer group IDs for cluster {CLUSTER_ID}...")
    response = make_request(conn, "GET", f"/kafka/v3/clusters/{CLUSTER_ID}/consumer-groups")
    if not response:
        logging.error("No consumer group ids found")
        return []
    consumer_group_ids = [item["consumer_group_id"] for item in response.get("data", [])]
    logging.info(f"Found {len(consumer_group_ids)} consumer groups")
    return consumer_group_ids


def get_consumer_group_state(conn: http.client.HTTPSConnection, consumer_group_id) -> str:
    response = make_request(conn, "GET", f"/kafka/v3/clusters/{CLUSTER_ID}/consumer-groups/{consumer_group_id}")
    if 'state' in response:
        return response['state']
    else:
        logging.error(f"Unable to determine the state of consumer group '{consumer_group_id}'")
        return None


def log_state_change(consumer_group_id, new_state):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with file_lock:
        with open("consumer_group_states.csv", mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, consumer_group_id, new_state])


def poll(consumer_group_ids: List[str]):
    conn = http.client.HTTPSConnection(ENDPOINT)
    try:
        for consumer_group_id in consumer_group_ids:
            current_state = get_consumer_group_state(conn, consumer_group_id)
            if current_state != consumer_groups_states[consumer_group_id]:
                log_state_change(consumer_group_id, current_state)
                with consumer_groups_states_lock:
                    consumer_groups_states[consumer_group_id] = current_state
    finally:
        conn.close()


if __name__ == '__main__':
    conn = http.client.HTTPSConnection(ENDPOINT)
    consumer_groups_states = {}
    timeout = time.time() + 60 * MINUTES
    try:
        consumer_group_ids = get_consumer_group_ids(conn)
        for consumer_group_id in consumer_group_ids:
            consumer_groups_states[consumer_group_id] = None

        consumer_group_subgroups = np.array_split(consumer_group_ids, THREADS)

        logging.info(f"Monitoring consumer group states for {MINUTES} minutes...")

        while time.time() < timeout:
            with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
                futures = [executor.submit(poll, subgroup) for subgroup in consumer_group_subgroups]
                concurrent.futures.wait(futures)
    finally:
        conn.close()

    logging.info(f"Monitoring completed.")

# TODO: PERSIST / AGGRREGATE / DASHBOARD THE STATE OF THE GROUPS