import time
import uuid
import json
import csv
import random
import numpy as np
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from pymongo import MongoClient
from arango import ArangoClient
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.management.buckets import CreateBucketSettings
from couchbase.exceptions import BucketAlreadyExistsException
import argparse

# Конфігурація
THREADS = 10
TIMEOUT = 120  # Таймаут для операцій в секундах
PAUSE_BETWEEN_EXPERIMENTS = 30  # Пауза між експериментами в секундах
TEST_DOC = {
    "name": "Test",
    "value": 123,
    "uuid": None,
    "data": None  # Буде заповнено даними різного розміру
}

# Сценарії навантаження
WORKLOAD_SCENARIOS = {
    "read_heavy": {"read": 90, "write": 10, "description": "90% читання, 10% запису"},
    "balanced": {"read": 50, "write": 50, "description": "50% читання, 50% запису"},
    "write_heavy": {"read": 10, "write": 90, "description": "10% читання, 90% запису"},
    "read_only": {"read": 100, "write": 0, "description": "100% читання"},
    "write_only": {"read": 0, "write": 100, "description": "100% запису"},
    "batch_write": {"read": 0, "write": 100, "description": "Пакетний запис"},
    "complex_query": {"read": 100, "write": 0, "description": "Складні запити"}
}

# Розміри документів (в КБ)
DOCUMENT_SIZES = {
    "small": {"size": 1, "description": "1KB"},
    "medium": {"size": 10, "description": "10KB"},
    "large": {"size": 100, "description": "100KB"},
    "xlarge": {"size": 1000, "description": "1MB"}
}

# Доступні бази даних
AVAILABLE_DATABASES = ["mongodb", "arangodb", "couchbase"]

def get_db_connection(db_name):
    """Отримання підключення до бази даних"""
    if db_name == "mongodb":
        print("🔌 Підключення до MongoDB...")
        client = MongoClient("mongodb://localhost:27017/")
        db = client.benchmark
        collection = db.test
        collection.delete_many({})  # Очищення колекції
        
        return {
            "insert_fn": lambda doc: collection.insert_one(doc),
            "read_fn": lambda: list(collection.find({"name": "Test"}))
        }
        
    elif db_name == "arangodb":
        print("🔌 Підключення до ArangoDB...")
        client = ArangoClient()
        db = client.db('_system', username='root', password='admin')
        
        if not db.has_database('benchmark'):
            db.create_database('benchmark')
        db = client.db('benchmark', username='root', password='admin')
        
        if db.has_collection('test'):
            db.delete_collection('test')
        col = db.create_collection('test')
        
        return {
            "insert_fn": lambda doc: col.insert(doc),
            "read_fn": lambda: list(col.find({"name": "Test"}))
        }
        
    elif db_name == "couchbase":
        print("🔌 Підключення до Couchbase...")
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
                    pass
        
        return {
            "insert_fn": insert,
            "read_fn": read
        }
    else:
        raise ValueError(f"Непідтримувана база даних: {db_name}")

class SystemMetrics:
    """Клас для збору системних метрик"""
    def __init__(self):
        self.metrics = []
        self.running = False
        self.thread = None

    def start(self):
        """Запуск збору метрик"""
        self.running = True
        self.thread = threading.Thread(target=self._collect_metrics)
        self.thread.start()

    def stop(self):
        """Зупинка збору метрик"""
        self.running = False
        if self.thread:
            self.thread.join()

    def _collect_metrics(self):
        """Збір системних метрик"""
        while self.running:
            metrics = {
                "timestamp": time.time(),
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_io": psutil.disk_io_counters(),
                "net_io": psutil.net_io_counters()
            }
            self.metrics.append(metrics)
            time.sleep(1)

    def get_average_metrics(self):
        """Отримання середніх значень метрик"""
        if not self.metrics:
            return None

        return {
            "avg_cpu": np.mean([m["cpu_percent"] for m in self.metrics]),
            "avg_memory": np.mean([m["memory_percent"] for m in self.metrics]),
            "avg_disk_read": np.mean([m["disk_io"].read_bytes for m in self.metrics]),
            "avg_disk_write": np.mean([m["disk_io"].write_bytes for m in self.metrics]),
            "avg_net_sent": np.mean([m["net_io"].bytes_sent for m in self.metrics]),
            "avg_net_recv": np.mean([m["net_io"].bytes_recv for m in self.metrics])
        }

def create_test_doc(size_kb):
    """Створення тестового документа заданого розміру"""
    doc = TEST_DOC.copy()
    doc["uuid"] = str(uuid.uuid4())
    # Генерація даних заданого розміру
    doc["data"] = "x" * (size_kb * 1024)  # Конвертуємо КБ в байти
    return doc

def generate_document_sizes(max_docs):
    """Генерація масиву розмірів документів з 10 кроками"""
    return np.geomspace(1000, max_docs, 10, dtype=int)

def run_benchmark(db_name, scenario_name, max_docs, doc_size):
    """Запуск комплексного бенчмарку"""
    print(f"\n🚀 Запуск бенчмарку для {db_name}")
    print(f"📊 Сценарій: {scenario_name}")
    print(f"📦 Розмір документу: {doc_size['description']}")
    
    scenario = WORKLOAD_SCENARIOS[scenario_name]
    results = []
    
    # Генерація розмірів наборів даних
    doc_sizes = generate_document_sizes(max_docs)
    print(f"\n📈 Буде протестовано наступні розміри наборів: {doc_sizes}")
    
    # Ініціалізація системних метрик
    system_metrics = SystemMetrics()
    
    # Отримання підключення до БД
    db_connection = get_db_connection(db_name)
    insert_fn = lambda doc: db_connection["insert_fn"](doc)
    read_fn = db_connection["read_fn"]
    
    # Запуск тестування для кожного розміру набору
    for num_docs in doc_sizes:
        print(f"\n📊 Тестування з {num_docs} документами...")
        
        # Запуск збору метрик
        system_metrics.start()
        
        # Підготовка документів
        docs = [create_test_doc(doc_size["size"]) for _ in range(num_docs)]
        
        # Вимірювання часу
        start_time = time.time()
        timeout_occurred = False
        
        try:
            with ThreadPoolExecutor(max_workers=THREADS) as executor:
                if scenario_name == "batch_write":
                    # Пакетний запис
                    futures = [executor.submit(insert_fn, doc) for doc in docs]
                    for future in futures:
                        future.result(timeout=TIMEOUT)
                elif scenario_name == "complex_query":
                    # Складні запити
                    futures = [executor.submit(read_fn) for _ in range(num_docs)]
                    for future in futures:
                        future.result(timeout=TIMEOUT)
                else:
                    # Звичайне змішане навантаження
                    read_ops = int(num_docs * (scenario["read"] / 100))
                    write_ops = num_docs - read_ops
                    
                    ops = []
                    for _ in range(write_ops):
                        doc = create_test_doc(doc_size["size"])
                        ops.append(("write", doc))
                    
                    for _ in range(read_ops):
                        ops.append(("read", None))
                    
                    random.shuffle(ops)
                    
                    futures = []
                    for op_type, doc in ops:
                        if op_type == "write":
                            futures.append(executor.submit(insert_fn, doc))
                        else:
                            futures.append(executor.submit(read_fn))
                    
                    for future in futures:
                        future.result(timeout=TIMEOUT)
        
        except TimeoutError:
            print(f"⚠️ Таймаут при виконанні операцій з {num_docs} документами")
            timeout_occurred = True
        
        total_time = time.time() - start_time
        
        # Зупинка збору метрик
        system_metrics.stop()
        avg_metrics = system_metrics.get_average_metrics()
        
        # Розрахунок метрик
        if timeout_occurred:
            throughput = 0
            avg_latency = TIMEOUT
        else:
            throughput = num_docs / total_time
            avg_latency = total_time / num_docs
        
        results.append({
            "database": db_name,
            "scenario": scenario_name,
            "document_size": doc_size["description"],
            "documents": num_docs,
            "total_time": total_time,
            "throughput": throughput,
            "avg_latency": avg_latency,
            "read_percentage": scenario["read"],
            "write_percentage": scenario["write"],
            "avg_cpu": avg_metrics["avg_cpu"],
            "avg_memory": avg_metrics["avg_memory"],
            "avg_disk_read": avg_metrics["avg_disk_read"],
            "avg_disk_write": avg_metrics["avg_disk_write"],
            "avg_net_sent": avg_metrics["avg_net_sent"],
            "avg_net_recv": avg_metrics["avg_net_recv"],
            "timeout_occurred": timeout_occurred
        })
        
        # Пауза між експериментами
        if num_docs != doc_sizes[-1]:  # Не чекаємо після останнього експерименту
            print(f"⏳ Очікування {PAUSE_BETWEEN_EXPERIMENTS} секунд перед наступним експериментом...")
            time.sleep(PAUSE_BETWEEN_EXPERIMENTS)
    
    # Збереження результатів
    filename = f"benchmark_{db_name}_{scenario_name}_{doc_size['size']}kb.csv"
    with open(filename, mode="w", newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"\n✅ Результати збережено у файл {filename}")
    
    return results

def run_all_benchmarks():
    """Запуск всіх бенчмарків для всіх баз даних"""
    all_results = []
    
    for db_name in AVAILABLE_DATABASES:
        print(f"\n🔍 Початок тестування бази даних: {db_name}")
        
        for scenario_name in WORKLOAD_SCENARIOS.keys():
            for doc_size_name, doc_size in DOCUMENT_SIZES.items():
                try:
                    results = run_benchmark(db_name, scenario_name, 5000, doc_size)
                    all_results.extend(results)
                except Exception as e:
                    print(f"❌ Помилка при тестуванні {db_name} з сценарієм {scenario_name}: {str(e)}")
                    continue
        
        print(f"\n✅ Завершено тестування бази даних: {db_name}")
        print(f"⏳ Очікування {PAUSE_BETWEEN_EXPERIMENTS} секунд перед наступною базою даних...")
        time.sleep(PAUSE_BETWEEN_EXPERIMENTS)
    
    # Збереження всіх результатів в один файл
    all_results_filename = "benchmark_all_results.csv"
    with open(all_results_filename, mode="w", newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=all_results[0].keys())
        writer.writeheader()
        writer.writerows(all_results)
    print(f"\n✅ Всі результати збережено у файл {all_results_filename}")

def main():
    parser = argparse.ArgumentParser(description='Комплексний інструмент бенчмарку NoSQL баз даних')
    parser.add_argument('--mode', choices=['single', 'all'], default='all',
                      help='Режим роботи: single - один тест, all - всі тести')
    parser.add_argument('--db', choices=AVAILABLE_DATABASES,
                      help='База даних для тестування (тільки для режиму single)')
    parser.add_argument('--scenario', choices=WORKLOAD_SCENARIOS.keys(),
                      help='Сценарій навантаження (тільки для режиму single)')
    parser.add_argument('--max-docs', type=int, default=5000,
                      help='Максимальна кількість документів для тестування')
    parser.add_argument('--doc-size', choices=DOCUMENT_SIZES.keys(), default='small',
                      help='Розмір тестових документів')
    
    args = parser.parse_args()
    
    if args.mode == 'all':
        run_all_benchmarks()
    else:
        if not args.db or not args.scenario:
            parser.error("Для режиму single потрібно вказати --db та --scenario")
        run_benchmark(args.db, args.scenario, args.max_docs, DOCUMENT_SIZES[args.doc_size])

if __name__ == "__main__":
    main() 