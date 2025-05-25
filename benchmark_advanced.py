import time
import uuid
import json
import csv
import random
from statistics import mean
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient
import requests
from arango import ArangoClient
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.buckets import CreateBucketSettings
from couchbase.exceptions import BucketAlreadyExistsException
import matplotlib.pyplot as plt
import pandas as pd

DOCUMENT_SIZES = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096]
THREADS = 10
TEST_DOC = {
    "name": "Test",
    "value": 123,
    "uuid": None  # буде генеруватися у кожного документа
}

# Сценарії навантаження: запис/читання у відсотках
WORKLOAD_SCENARIOS = [
    {"name": "read_heavy", "read": 90, "write": 10, "description": "90% читання, 10% запису"},
    {"name": "balanced", "read": 50, "write": 50, "description": "50% читання, 50% запису"},
    {"name": "write_heavy", "read": 10, "write": 90, "description": "10% читання, 90% запису"}
]

def create_test_doc():
    doc = TEST_DOC.copy()
    doc["uuid"] = str(uuid.uuid4())
    return doc

def insert_parallel(fn_insert, num_docs, db_name):
    docs = [create_test_doc() for _ in range(num_docs)]

    start = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(fn_insert, doc) for doc in docs]
        for future in futures:
            future.result()
    total_time = time.time() - start
    print(f"[{db_name}] Insert {num_docs} docs done in {total_time:.2f}s")
    return total_time, total_time / num_docs

def read_parallel(fn_read, num_docs, db_name):
    start = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = [executor.submit(fn_read) for _ in range(num_docs)]
        for future in futures:
            future.result()
    total_time = time.time() - start
    print(f"[{db_name}] Read {num_docs} docs done in {total_time:.2f}s")
    return total_time, total_time / num_docs

def mixed_workload_parallel(fn_insert, fn_read, num_ops, read_pct, write_pct, db_name):
    read_ops = int(num_ops * (read_pct / 100))
    write_ops = num_ops - read_ops
    
    ops = []
    # Підготувати операції запису
    for _ in range(write_ops):
        doc = create_test_doc()
        ops.append(("write", doc))
    
    # Підготувати операції читання
    for _ in range(read_ops):
        ops.append(("read", None))
    
    # Перемішати операції для реалістичнішого навантаження
    random.shuffle(ops)
    
    start = time.time()
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for op_type, doc in ops:
            if op_type == "write":
                futures.append(executor.submit(fn_insert, doc))
            else:
                futures.append(executor.submit(fn_read))
        
        for future in futures:
            future.result()
    
    total_time = time.time() - start
    print(f"[{db_name}] Mixed workload ({read_pct}% read, {write_pct}% write) - {num_ops} ops done in {total_time:.2f}s")
    return total_time, total_time / num_ops

def benchmark_mongo(num_docs, scenario=None):
    print(f"[MongoDB] Connecting...")
    client = MongoClient("mongodb://localhost:27017/")
    db = client.benchmark
    collection = db.test
    collection.delete_many({})  # Clean

    insert_fn = lambda doc: collection.insert_one(doc)
    read_fn = lambda: list(collection.find({"name": "Test"}))

    if scenario:
        # Змішане навантаження
        workload_time, avg_op_time = mixed_workload_parallel(
            insert_fn, read_fn, num_docs, 
            scenario["read"], scenario["write"], f"MongoDB - {scenario['description']}"
        )
        return workload_time, avg_op_time
    else:
        # Оригінальне роздільне тестування
        insert_time, avg_insert = insert_parallel(insert_fn, num_docs, "MongoDB")
        read_time, avg_read = read_parallel(read_fn, num_docs, "MongoDB")
        return insert_time, read_time, avg_insert, avg_read

# def benchmark_couchdb(num_docs, scenario=None):
#     print(f"[CouchDB] Connecting...")
#     base_url = "http://admin:admin@localhost:5984/benchmark"
#     requests.put(base_url)
#     headers = {"Content-Type": "application/json"}
# 
#     def insert(doc):
#         requests.post(base_url, headers=headers, data=json.dumps(doc))
# 
#     def read():
#         requests.get(base_url + "/_all_docs?include_docs=true")
# 
#     if scenario:
#         # Змішане навантаження
#         workload_time, avg_op_time = mixed_workload_parallel(
#             insert, read, num_docs, 
#             scenario["read"], scenario["write"], f"CouchDB - {scenario['description']}"
#         )
#         return workload_time, avg_op_time
#     else:
#         # Оригінальне роздільне тестування
#         insert_time, avg_insert = insert_parallel(insert, num_docs, "CouchDB")
#         read_time, avg_read = read_parallel(read, num_docs, "CouchDB")
#         return insert_time, read_time, avg_insert, avg_read

def benchmark_arango(num_docs, scenario=None):
    print(f"[ArangoDB] Connecting...")
    client = ArangoClient()
    db = client.db('_system', username='root', password='admin')

    if not db.has_database('benchmark'):
        db.create_database('benchmark')
    db = client.db('benchmark', username='root', password='admin')

    if db.has_collection('test'):
        db.delete_collection('test')
    col = db.create_collection('test')

    insert_fn = lambda doc: col.insert(doc)
    read_fn = lambda: list(col.find({"name": "Test"}))

    if scenario:
        # Змішане навантаження
        workload_time, avg_op_time = mixed_workload_parallel(
            insert_fn, read_fn, num_docs, 
            scenario["read"], scenario["write"], f"ArangoDB - {scenario['description']}"
        )
        return workload_time, avg_op_time
    else:
        # Оригінальне роздільне тестування
        insert_time, avg_insert = insert_parallel(insert_fn, num_docs, "ArangoDB")
        read_time, avg_read = read_parallel(read_fn, num_docs, "ArangoDB")
        return insert_time, read_time, avg_insert, avg_read

def benchmark_couchbase(num_docs, scenario=None):
    print(f"[Couchbase] Connecting...")
    cluster = Cluster("couchbase://localhost", ClusterOptions(
        PasswordAuthenticator("admin", "admin123")))
    bucket_name = "benchmark"
    try:
        cluster.buckets().create_bucket(CreateBucketSettings(name=bucket_name, ram_quota_mb=100))
    except BucketAlreadyExistsException:
        pass

    bucket = cluster.bucket(bucket_name)
    collection = bucket.default_collection()

    inserted_keys = []

    def insert(doc):
        key = str(uuid.uuid4())
        collection.upsert(key, doc)
        inserted_keys.append(key)

    def read():
        if inserted_keys:
            key = random.choice(inserted_keys)
            try:
                collection.get(key)
            except Exception:
                pass  # Якщо щось пішло не так — просто ігноруємо

    if scenario:
        # Для змішаного навантаження нам потрібно спочатку вставити документи для читання
        pre_inserted = min(100, num_docs)  # Попередньо вставити деякі документи для читання
        for _ in range(pre_inserted):
            insert(create_test_doc())
        
        # Змішане навантаження
        workload_time, avg_op_time = mixed_workload_parallel(
            insert, read, num_docs, 
            scenario["read"], scenario["write"], f"Couchbase - {scenario['description']}"
        )
        return workload_time, avg_op_time
    else:
        # Оригінальне роздільне тестування
        insert_time, avg_insert = insert_parallel(insert, num_docs, "Couchbase")
        read_time, avg_read = read_parallel(read, num_docs, "Couchbase")
        return insert_time, read_time, avg_insert, avg_read

def save_results(results, filename="benchmark_pro_results.csv"):
    with open(filename, mode="w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Database", "Documents", "Insert Time (s)", "Read Time (s)",
                         "Avg Insert Latency (s)", "Avg Read Latency (s)"])
        for row in results:
            writer.writerow(row)
    print(f"\n✅ Results saved to {filename}")

def plot_workload_results(results_df):
    # Створюємо графіки для кожного сценарію навантаження
    scenarios = results_df['Scenario'].unique()
    
    for scenario in scenarios:
        plt.figure(figsize=(12, 6))
        scenario_data = results_df[results_df['Scenario'] == scenario]
        
        for db in scenario_data['Database'].unique():
            db_data = scenario_data[scenario_data['Database'] == db]
            plt.plot(db_data['Documents'], db_data['Avg Operation Latency (s)'], 
                    marker='o', label=db)
        
        plt.title(f'Середній час операції для сценарію: {scenario}')
        plt.xlabel('Кількість операцій')
        plt.ylabel('Середній час операції (секунди)')
        plt.xscale('log', base=2)
        plt.grid(True)
        plt.legend()
        
        # Зберігаємо графік у кореневій директорії
        filename = f'benchmark_{scenario.lower().replace(" ", "_")}.png'
        plt.savefig(filename)
        plt.close()
        print(f"✅ Графік збережено у файл: {filename}")

def save_workload_results(results, filename="benchmark_workload_results.csv"):
    with open(filename, mode="w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["Database", "Scenario", "Documents", "Total Time (s)", "Avg Operation Latency (s)"])
        for row in results:
            writer.writerow(row)
    print(f"\n✅ Results saved to {filename}")
    
    # Створюємо графіки
    results_df = pd.read_csv(filename, encoding='utf-8')
    plot_workload_results(results_df)
    print("\n✅ Графіки збережено у файли benchmark_*.png")

def run_benchmark_pro():
    all_results = []

    for num_docs in DOCUMENT_SIZES:
        print(f"\n🚀 Benchmarking {num_docs} documents...\n")

        all_results.append(["MongoDB", num_docs] + list(benchmark_mongo(num_docs)))
        # all_results.append(["CouchDB", num_docs] + list(benchmark_couchdb(num_docs)))
        all_results.append(["ArangoDB", num_docs] + list(benchmark_arango(num_docs)))
        all_results.append(["Couchbase", num_docs] + list(benchmark_couchbase(num_docs)))

    save_results(all_results)

def run_workload_benchmarks():
    all_results = []
    
    for scenario in WORKLOAD_SCENARIOS:
        print(f"\n🚀 Running {scenario['description']} benchmark...\n")
        
        for num_docs in DOCUMENT_SIZES:
            print(f"\n📊 Testing with {num_docs} operations...\n")
            
            # MongoDB
            time_mongo, avg_mongo = benchmark_mongo(num_docs, scenario)
            all_results.append(["MongoDB", scenario['description'], num_docs, time_mongo, avg_mongo])
            
            # CouchDB
            # time_couch, avg_couch = benchmark_couchdb(num_docs, scenario)
            # all_results.append(["CouchDB", scenario['description'], num_docs, time_couch, avg_couch])
            
            # ArangoDB
            time_arango, avg_arango = benchmark_arango(num_docs, scenario)
            all_results.append(["ArangoDB", scenario['description'], num_docs, time_arango, avg_arango])
            
            # Couchbase
            time_couchbase, avg_couchbase = benchmark_couchbase(num_docs, scenario)
            all_results.append(["Couchbase", scenario['description'], num_docs, time_couchbase, avg_couchbase])
    
    save_workload_results(all_results)

if __name__ == "__main__":
    print("🚀 Запуск бенчмарків...")
    run_benchmark_pro()  # Запускаємо оригінальний бенчмарк
    run_workload_benchmarks()  # Запускаємо бенчмарк з різними сценаріями навантаження
    print("\n✅ Всі бенчмарки завершено!")
