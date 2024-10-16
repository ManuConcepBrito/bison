import json
import matplotlib.pyplot as plt
import numpy as np
import typer

# Initialize typer app
app = typer.Typer()


@app.command()
def plot_benchmark(file_name: str):
    """
    Plot the benchmark results from a given JSON file.

    Args:
        file_name: The name of the JSON file containing the benchmark results.
    """
    # Load benchmark data from the JSON file
    with open(file_name, "r") as f:
        data = json.load(f)

    experiments = ["50% Reads, 50% Updates", "95% Reads, 5% Updates"]
    databases = ["Bison", "TinyDB"]
    median_times = {
        experiment: {db: None for db in databases} for experiment in experiments
    }

    # Extract median times from the benchmarks
    for benchmark in data["benchmarks"]:
        param = benchmark["param"]  # Experiment type
        if "tinydb" in benchmark["name"].lower():
            db = "TinyDB"
        elif "bisondb" in benchmark["name"].lower():
            db = "Bison"
        else:
            continue

        # Get the median time
        median_time = benchmark["stats"]["median"]

        # Store the median time in the dictionary
        if param in experiments:
            median_times[param][db] = median_time

    # Plotting
    x = np.arange(len(experiments))
    width = 0.25

    fig, ax = plt.subplots(figsize=(8, 6))

    # Get the Bison and TinyDB median times for each experiment
    bisondb_medians = [median_times[exp]["Bison"] for exp in experiments]
    tinydb_medians = [median_times[exp]["TinyDB"] for exp in experiments]

    # Bison bars first
    rects1 = ax.bar(
        x - width / 2, bisondb_medians, width, label="Bison", color="#33a02c"
    )
    rects2 = ax.bar(
        x + width / 2, tinydb_medians, width, label="TinyDB", color="#1f78b4"
    )

    # Add labels, title, and custom x-axis tick labels
    ax.set_xlabel("Experiment")
    ax.set_ylabel("Median Time (seconds)")
    ax.set_title("Database Performance Comparison (Median Time)")
    ax.set_xticks(x)
    ax.set_xticklabels(experiments)
    ax.legend()

    # Add text for labels on the bars
    def autolabel(rects):
        """Attach a text label above each bar in *rects*, displaying its height."""
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                f"{height:.3f}",
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),  # 3 points vertical offset
                textcoords="offset points",
                ha="center",
                va="bottom",
            )

    autolabel(rects1)
    autolabel(rects2)

    # Display the plot
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    app()
