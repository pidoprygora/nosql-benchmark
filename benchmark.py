import time
import uuid
import json
import csv
from statistics import mean
from pymongo import MongoClient
import requests
from arango import ArangoClient
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.buckets import CreateBucketSettings
from couchbase.exceptions import BucketAlreadyExistsException

TEST_DOC = {
    "name": "Test",
    "value": 123,
    "uuid": str(uuid.uuid4())
}

ITERATIONS = 100


def time_op(fn, operation_name, db_name):
    durations = []
    print(f"[{db_name}] Starting {operation_name} for {ITERATIONS} iterations...")
    for _ in range(ITERATIONS):
        start = time.time()
        fn()
        durations.append(time.time() - start)
    total_time = sum(durations)
    avg_time = mean(durations)
    print(f"[{db_name}] Finished {operation_name}: total={total_time:.4f}s, avg={avg_time:.6f}s")
    return total_time, avg_time


def benchmark_mongo():
    print("[MongoDB] Connecting...")
    client = MongoClient("mongodb://localhost:27017/")
    db = client.benchmark
    collection = db.test
    collection.delete_many({})  # Clean

    insert_time, avg_insert = time_op(lambda: collection.insert_one(TEST_DOC.copy()), "insert", "MongoDB")
    read_time, avg_read = time_op(lambda: list(collection.find({"name": "Test"})), "read", "MongoDB")
    return insert_time, read_time, avg_insert, avg_read


def benchmark_couchdb():
    print("[CouchDB] Connecting...")
    base_url = "http://admin:admin@localhost:5984/benchmark"
    requests.put(base_url)
    headers = {"Content-Type": "application/json"}

    def insert():
        doc = TEST_DOC.copy()
        requests.post(base_url, headers=headers, data=json.dumps(doc))

    def read():
        requests.get(base_url + "/_all_docs?include_docs=true")

    insert_time, avg_insert = time_op(insert, "insert", "CouchDB")
    read_time, avg_read = time_op(read, "read", "CouchDB")
    return insert_time, read_time, avg_insert, avg_read


def benchmark_arango():
    print("[ArangoDB] Connecting...")
    client = ArangoClient()
    db = client.db('_system', username='root', password='admin')

    if not db.has_database('benchmark'):
        db.create_database('benchmark')
    db = client.db('benchmark', username='root', password='admin')

    if db.has_collection('test'):
        db.delete_collection('test')
    col = db.create_collection('test')

    insert_time, avg_insert = time_op(lambda: col.insert(TEST_DOC.copy()), "insert", "ArangoDB")
    read_time, avg_read = time_op(lambda: list(col.find({"name": "Test"})), "read", "ArangoDB")
    return insert_time, read_time, avg_insert, avg_read


def benchmark_couchbase():
    print("[Couchbase] Connecting...")
    cluster = Cluster("couchbase://localhost", ClusterOptions(
        PasswordAuthenticator("admin", "admin123")))
    bucket_name = "benchmark"
    try:
        cluster.buckets().create_bucket(CreateBucketSettings(name=bucket_name, ram_quota_mb=100))
    except BucketAlreadyExistsException:
        pass

    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    def insert():
        key = f"doc_{uuid.uuid4()}"
        collection.upsert(key, TEST_DOC.copy())

    def read():
        key = f"doc_not_existing"
        try:
            collection.get(key)
        except:
            pass

    insert_time, avg_insert = time_op(insert, "insert", "Couchbase")
    read_time, avg_read = time_op(read, "read", "Couchbase")
    return insert_time, read_time, avg_insert, avg_read


def save_results(results):
    with open("results/benchmark_results.csv", mode="w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Database", "Insert Time (s)", "Read Time (s)",
                         "Avg Insert Latency (s)", "Avg Read Latency (s)"])
        for db, metrics in results.items():
            writer.writerow([db] + [f"{m:.6f}" for m in metrics])
    print("\n✅ Results saved to benchmark_results.csv")


def run_benchmark():
    print("🚀 Starting NoSQL Benchmark...\n")

    results = {
        "MongoDB": benchmark_mongo(),
        "CouchDB": benchmark_couchdb(),
        "ArangoDB": benchmark_arango(),
        "Couchbase": benchmark_couchbase(),
    }

    save_results(results)
    print("\n🎯 Benchmark complete!")
