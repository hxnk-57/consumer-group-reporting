import http.client
import json
import logging
import time
import streamlit

CLUSTER_ID = "bF3Cdi49R3SMRTx-gWnm9g"
CONSUMER_GROUP_ID = ""
ENDPOINT =  "localhost"
PORT = 8082
MINUTES = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def make_request(conn: http.client.HTTPConnection, method: str, endpoint: str) -> dict:
    try:
        conn.request(method, endpoint)
        response = conn.getresponse()
        data_str = response.read().decode("utf-8")
        return json.loads(data_str)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {endpoint}: {e}")
    except Exception as e:
        logging.error(f"Error making request to {endpoint}: {e}")
    return {}


def get_consumer_group_ids(conn: http.client.HTTPConnection) -> list[str]:
    logging.info(f"Fetching consumer group IDs for cluster {CLUSTER_ID}...")
    response = make_request(conn, "GET", f"/v3/clusters/{CLUSTER_ID}/consumer-groups")
    if not response:
        logging.error("No consumer group ids found")
        return []
    consumer_group_ids = [item["consumer_group_id"] for item in response.get("data", [])]
    logging.info(f"Found {len(consumer_group_ids)} consumer groups")
    return consumer_group_ids


def get_consumer_group_state(conn: http.client.HTTPConnection, consumer_group_id) -> str:
    response = make_request(conn, "GET", f"/v3/clusters/{CLUSTER_ID}/consumer-groups/{consumer_group_id}")
    if 'state' in response:
        return response['state']
    else:
        logging.error(f"Unable to determine the state of consumer group '{consumer_group_id}'")
        return None


def get_consumer_ids(conn: http.client.HTTPConnection, consumer_group_id: str) -> list[str]:
    response = make_request(conn, "GET", f"/v3/clusters/{CLUSTER_ID}/consumer-groups/{consumer_group_id}/consumers")
    if 'data' in response and isinstance(response['data'], list):
        return [consumer.get('consumer_id') for consumer in response['data'] if consumer.get('consumer_id')]

    logging.error("No consumer data found in the consumer group.")
    return []


def poll(conn: http.client.HTTPConnection, consumer_group_ids: str):
    for consumer_group_id in consumer_group_ids:
        consumer_ids = get_consumer_ids(conn, consumer_group_id)
        consumer_groups_members[consumer_group_id] = set(consumer_ids)

        current_state = get_consumer_group_state(conn, consumer_group_id)
        if current_state != consumer_groups_states[consumer_group_id]:
            logging.info(f"Consumer Group '{consumer_group_id}' is {current_state}")
            consumer_groups_states[consumer_group_id] = current_state


if __name__ == '__main__':

    conn = http.client.HTTPConnection(ENDPOINT, PORT)
    consumer_groups_members = {}
    consumer_groups_states = {}
    timeout = time.time() + 60 * MINUTES

    try:
        consumer_group_ids = get_consumer_group_ids(conn)
        for consumer_group_id in consumer_group_ids:
            consumer_groups_states[consumer_group_id] = None

        while time.time() < timeout:
            poll(conn, consumer_group_ids)
    finally:
        conn.close()


    for group_id, consumer_ids in consumer_groups_members.items():
        print(f"In the last {MINUTES} minutes consumer Group: '{group_id}' had {len(consumer_ids)} unique consumers:")
        for consumer_id in consumer_ids:
            print(consumer_id)