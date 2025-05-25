import pandas as pd
import matplotlib.pyplot as plt

def visualize_pro(csv_file="benchmark_pro_results.csv"):
    print("📊 Visualizing PRO benchmark results...")
    df = pd.read_csv(csv_file, encoding='utf-8')

    databases = df["Database"].unique()
    doc_sizes = sorted(df["Documents"].unique())

    # Підготуємо дані для Insert Time
    insert_data = {db: [] for db in databases}
    for db in databases:
        for size in doc_sizes:
            time_value = df[(df["Database"] == db) & (df["Documents"] == size)]["Insert Time (s)"].values[0]
            insert_data[db].append(time_value)

    # Підготуємо дані для Read Time
    read_data = {db: [] for db in databases}
    for db in databases:
        for size in doc_sizes:
            time_value = df[(df["Database"] == db) & (df["Documents"] == size)]["Read Time (s)"].values[0]
            read_data[db].append(time_value)

    # Кольори для баз
    colors = ["#4CAF50", "#2196F3", "#FFC107", "#F44336"]

    # INSERT TIME
    plt.figure(figsize=(10, 6))
    for idx, db in enumerate(databases):
        plt.bar(
            [x + idx * 0.2 for x in range(len(doc_sizes))],  # зсув для кожної бази
            insert_data[db],
            width=0.2,
            label=db,
            color=colors[idx % len(colors)]
        )
    plt.title("Insert Time per Database (Linear Scale)")
    plt.xlabel("Number of Documents")
    plt.ylabel("Total Insert Time (s)")
    plt.xticks([r + 0.3 for r in range(len(doc_sizes))], doc_sizes)
    plt.legend()
    plt.grid(axis="y", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("insert_time_pro.png")
    plt.close()

    # READ TIME
    plt.figure(figsize=(10, 6))
    for idx, db in enumerate(databases):
        plt.bar(
            [x + idx * 0.2 for x in range(len(doc_sizes))],
            read_data[db],
            width=0.2,
            label=db,
            color=colors[idx % len(colors)]
        )
    plt.title("Read Time per Database (Linear Scale)")
    plt.xlabel("Number of Documents")
    plt.ylabel("Total Read Time (s)")
    plt.xticks([r + 0.3 for r in range(len(doc_sizes))], doc_sizes)
    plt.legend()
    plt.grid(axis="y", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("read_time_pro.png")
    plt.close()

    print("✅ Visualization complete. Images saved: insert_time_pro.png, read_time_pro.png")

def visualize_workload(csv_file="benchmark_workload_results.csv"):
    print("📊 Visualizing workload benchmark results...")
    df = pd.read_csv(csv_file, encoding='utf-8')
    
    # Кольори для баз даних
    colors = ["#4CAF50", "#2196F3", "#FFC107", "#F44336"]
    
    # Створюємо графіки для кожного сценарію
    scenarios = df["Scenario"].unique()
    doc_sizes = sorted(df["Documents"].unique())
    
    for scenario in scenarios:
        scenario_data = df[df["Scenario"] == scenario]
        
        # 1. Графік середнього часу операції
        plt.figure(figsize=(12, 6))
        for idx, db in enumerate(scenario_data["Database"].unique()):
            db_data = scenario_data[scenario_data["Database"] == db]
            db_data = db_data.sort_values("Documents")
            plt.plot(
                db_data["Documents"],
                db_data["Avg Operation Latency (s)"],
                marker='o',
                label=db,
                color=colors[idx % len(colors)],
                linewidth=2
            )
        plt.title(f"Середній час операції для сценарію: {scenario}")
        plt.xlabel("Кількість операцій")
        plt.ylabel("Середній час операції (секунди)")
        plt.xscale('log', base=2)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        filename = f'benchmark_{scenario.lower().replace(" ", "_")}_avg_latency.png'
        plt.savefig(filename)
        plt.close()
        print(f"✅ Графік середнього часу збережено у файл: {filename}")

        # 2. Графік загального часу
        plt.figure(figsize=(12, 6))
        for idx, db in enumerate(scenario_data["Database"].unique()):
            db_data = scenario_data[scenario_data["Database"] == db]
            db_data = db_data.sort_values("Documents")
            plt.plot(
                db_data["Documents"],
                db_data["Total Time (s)"],
                marker='o',
                label=db,
                color=colors[idx % len(colors)],
                linewidth=2
            )
        plt.title(f"Загальний час виконання для сценарію: {scenario}")
        plt.xlabel("Кількість операцій")
        plt.ylabel("Загальний час (секунди)")
        plt.xscale('log', base=2)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.legend()
        plt.tight_layout()
        filename = f'benchmark_{scenario.lower().replace(" ", "_")}_total_time.png'
        plt.savefig(filename)
        plt.close()
        print(f"✅ Графік загального часу збережено у файл: {filename}")

        # 3. Графік у вигляді стовпчиків для порівняння баз даних
        plt.figure(figsize=(12, 6))
        db_data = scenario_data.groupby('Database')['Avg Operation Latency (s)'].mean()
        plt.bar(db_data.index, db_data.values, color=colors[:len(db_data)])
        plt.title(f"Порівняння середнього часу операції для сценарію: {scenario}")
        plt.xlabel("База даних")
        plt.ylabel("Середній час операції (секунди)")
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        filename = f'benchmark_{scenario.lower().replace(" ", "_")}_comparison.png'
        plt.savefig(filename)
        plt.close()
        print(f"✅ Графік порівняння збережено у файл: {filename}")

if __name__ == "__main__":
    visualize_pro()
    visualize_workload()