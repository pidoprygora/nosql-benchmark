import pandas as pd
import matplotlib.pyplot as plt

def visualize_results(csv_file="results/benchmark_results.csv"):
    print("📈 Visualizing benchmark results...")
    df = pd.read_csv(csv_file)

    databases = df["Database"]
    insert_times = df["Insert Time (s)"]
    read_times = df["Read Time (s)"]
    avg_insert_latencies = df["Avg Insert Latency (s)"]
    avg_read_latencies = df["Avg Read Latency (s)"]

    # Графік: Total Insert Time
    plt.figure(figsize=(10, 5))
    plt.bar(databases, insert_times)
    plt.title("Insert Time by Database (log scale)")
    plt.xlabel("Database")
    plt.ylabel("Total Insert Time (s)")
    plt.yscale("log")  # логарифмічна шкала
    plt.grid(axis="y", which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("plots/insert_times.png")
    plt.show()

    # Графік: Total Read Time
    plt.figure(figsize=(10, 5))
    plt.bar(databases, read_times)
    plt.title("Read Time by Database (log scale)")
    plt.xlabel("Database")
    plt.ylabel("Total Read Time (s)")
    plt.yscale("log")
    plt.grid(axis="y", which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("plots/read_times.png")
    plt.show()

    # Графік: Avg Insert Latency
    plt.figure(figsize=(10, 5))
    plt.bar(databases, avg_insert_latencies)
    plt.title("Average Insert Latency by Database (log scale)")
    plt.xlabel("Database")
    plt.ylabel("Avg Insert Latency (s)")
    plt.yscale("log")
    plt.grid(axis="y", which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("plots/avg_insert_latency.png")
    plt.show()

    # Графік: Avg Read Latency
    plt.figure(figsize=(10, 5))
    plt.bar(databases, avg_read_latencies)
    plt.title("Average Read Latency by Database (log scale)")
    plt.xlabel("Database")
    plt.ylabel("Avg Read Latency (s)")
    plt.yscale("log")
    plt.grid(axis="y", which="both", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    plt.savefig("plots/avg_read_latency.png")
    plt.show()

    print("✅ Visualization complete. Images saved: insert_times.png, read_times.png, avg_insert_latency.png, avg_read_latency.png.")
