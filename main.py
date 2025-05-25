from benchmark import run_benchmark
from visualization import visualize_results
from advanced_visualization import visualize_pro, visualize_workload
from benchmark_advanced import run_benchmark_pro, run_workload_benchmarks
from benchmark_new import run_benchmark as run_new_benchmark
import argparse

def run_all_benchmarks():
    """Запуск всіх типів бенчмарків"""
    print("🚀 Запуск бенчмарків...")
    
    # Запускаємо обидва типи бенчмарків
    run_benchmark_pro()  # Звичайний бенчмарк
    run_workload_benchmarks()  # Бенчмарк з різними сценаріями навантаження
    
    print("\n📊 Створення візуалізацій...")
    # Створюємо всі типи графіків
    visualize_pro()  # Графіки для звичайного бенчмарку
    visualize_workload()  # Графіки для різних сценаріїв навантаження
    
    print("\n✅ Всі бенчмарки та візуалізації завершено!")

def run_single_benchmark(db_name, scenario_name, num_docs):
    """Запуск одиночного бенчмарку з новим інструментом"""
    print(f"🚀 Запуск бенчмарку для {db_name} зі сценарієм {scenario_name}...")
    results = run_new_benchmark(db_name, scenario_name, num_docs)
    print("\n✅ Бенчмарк завершено!")
    return results

def main():
    parser = argparse.ArgumentParser(description='NoSQL Database Benchmark Suite')
    parser.add_argument('--mode', choices=['all', 'single'], default='all',
                      help='Режим роботи: all - всі бенчмарки, single - одиночний бенчмарк')
    parser.add_argument('--db', choices=['mongodb', 'arangodb', 'couchbase'],
                      help='База даних для тестування (тільки для режиму single)')
    parser.add_argument('--scenario', 
                      choices=['read_heavy', 'balanced', 'write_heavy', 'read_only', 'write_only'],
                      help='Сценарій навантаження (тільки для режиму single)')
    parser.add_argument('--docs', type=int, default=1000,
                      help='Кількість документів для тестування (тільки для режиму single)')
    
    args = parser.parse_args()
    
    if args.mode == 'all':
        run_all_benchmarks()
    else:
        if not args.db or not args.scenario:
            parser.error("Для режиму single потрібно вказати --db та --scenario")
        run_single_benchmark(args.db, args.scenario, args.docs)

if __name__ == "__main__":
    main()
