import json
import os

NUM_MESSAGES = "num_messages"
MESSAGE_INTERVAL = "message_broadcast_interval"
NUM_PAYMENTS = "num_payments"
PAYMENT_INTERVAL = "payment_interval"
PERCENT_BLACKHOLES = "percent_blackholes"
GOSSIP_ALGORITHM = "algo"
NUM_SYNCER_CONNECTIONS = "syncer_conns"
GRAPH_FILE = "graph"
OUTPUT_DIR = "output"
ID = "id"

DATA_NAME_TO_PATH = {
    "keepalive": "l2/keepalive.csv",
    "closures": "l2/closures.csv",
    "reopens": "l2/reopens.csv",
    "reopendelays": "l2/reopendelays.csv",
    "disruptive": "l2/disruptive.csv",
    "nondisruptive": "l2/nondisruptive.csv",
    "misc": "l2/misc.csv",
    "keepalive_times": "l2/keepalive_times.csv",
    "timeseen_past": "l2/timeseen_vs_timestamp_past.csv",
    "timeseen_future": "l2/timeseen_vs_timestamp_future.csv",
    "relativediffs": "l2/relativediffs.csv",
    "absolutediffs": "l2/absolutediffs.csv",
    "capacities": "l2/capacities.csv",

    "channel_updates": "l2/channel_updates.csv",
    "node_announcements": "l2/node_announcements.csv",
    "channel_announcements": "l2/channel_announcements.csv",
    "payments": "l2/payments.csv",
    "first_seen": "l2/seen.csv",
    "convbuckets": "l2/convbuckets.csv",
    "waitedbuckets": "l2/waitedbuckets.csv",
    "waited": "l2/waited.csv",
    "bandwidth": "l1/bandwidth.csv",
    "bandwidthbuckets": "l1/bandwidth_buckets.csv",
    "redundancy": "l1/redundancy.csv",
    "inflighttimes": "l1/inflighttime.csv",
}

class Config:
    def __init__(self, path):
        self.path = path
        self.load(path)

    def load(self, path):
        config_file = open(path, "r")
        self.data = json.load(config_file)

    def get_data_path(self, data_name):
        assert(data_name in DATA_NAME_TO_PATH)
        return os.path.join(self.data[OUTPUT_DIR], DATA_NAME_TO_PATH[data_name])

    def get_algo_name(self):
        return self.data[GOSSIP_ALGORITHM]["name"]
    
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return json.dumps(self.data, indent=4)
