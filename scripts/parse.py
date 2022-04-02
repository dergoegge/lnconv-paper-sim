import csv
import numpy

def get_percentile(counts, bins, percentile):
    total = sum(counts)
    cum = 0
    b = 0
    for count in counts:
        if (cum / total) >= percentile/100:
            return bins[b], cum
        cum += count
        b += 1

def print_percentile(name, counts, bins, percentile):
    t, c = get_percentile(counts, bins, percentile)
    print(name, percentile, t, c)


def to_seconds(milli):
    return milli/1000.0

# Read total bandwidth used data
# expects rows of from: timestamp, total_bandwidth
def parse_bandwidth(path):
    times = []
    band = []
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            times.append(to_seconds(int(row['timestamp'])))
            band.append(int(row['total_bandwidth']))
            count += 1

    return times, band

def parse_waited(path):
    waited_all = []
    not_dropped = []
    dropped = []
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            waited_all.append(int(row["waited"]))
            drop = row["dropped"]
            if drop == "true":
                dropped.append(int(row["waited"]))
            else:
                not_dropped.append(int(row["waited"]))

    return waited_all, not_dropped, dropped

def parse_convdelay(path):
    times = []

    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            times.append(int(row['timestamp']))

    return times

def parse_buckets(path, convert=to_seconds):
    bins = [0]
    count = []
    bucket_size = None

    if convert == None:
        convert = lambda x: x

    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if bucket_size == None:
                bucket_size = int(row["bucket_size"])

            bins.append(convert((int(row["bucket_id"])+1) * bucket_size))
            count.append(int(row["count"]))

    return bins+[numpy.inf], count+[0]


# Read payment data
def parse_payments(path):
    failure_times = {
        "LIQUIDITY": [],
        "NOROUTE": [],
        "CONV-OKOLD": [],
        "CONV-DISABLED": [],
        "CONV-FEEUP": [],
        "CONV-FEEDOWN": [],
        "CONV-CLTVUP": [],
        "CONV-CLTVDOWN": [],
    }

    payment_count = 0
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            splits = row['status'].split("|")
            payment_count += 1
            for status in splits:
                if status != "SUCCESS" and status != "":
                    if status in failure_times:
                        failure_times[status].append(int(row['timestamp']))

    all_fails = 0
    conv_count = 0
    for k in failure_times:
        print(k, len(failure_times[k]) * 100 / payment_count) 
        all_fails += len(failure_times[k])
        if "CONV" in k:
            conv_count += len(failure_times[k])

    print("success", all_fails * 100 / payment_count)
    print("conv", conv_count)

    return failure_times

