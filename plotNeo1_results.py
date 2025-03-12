import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load the results CSV file
results = pd.read_csv("performance_results.csv")

# Define the dataset size order
order = ["250k", "500k", "750k", "1000k"]

# Specify the database you want to plot for (e.g., "MySQL", "Cassandra", etc.)
database = "Neo4j"

# Filter results for the selected database
db_results = results[results["database"] == database]

# Get unique query numbers (assuming query_number is numeric: 1, 2, 3, 4)
queries = sorted(db_results["query_number"].unique())

# We'll create a grouped bar chart in a single subplot
fig, ax = plt.subplots(figsize=(9, 6))

# Create an array of x positions for each dataset size
x = np.arange(len(order))

# Width of each bar
width = 0.2

# Define a color palette for the queries (one color per query)
colors = ["#ECCEEC", "#B68B95", "#85597F", "#261C2B"]    

# Loop through each query and plot its bars
for i, q in enumerate(queries):
    # Filter data for query q
    query_data = db_results[db_results["query_number"] == q].copy()

    # Ensure dataset_size is categorical and sorted in the defined order
    query_data["dataset_size"] = pd.Categorical(query_data["dataset_size"], categories=order, ordered=True)
    query_data = query_data.sort_values("dataset_size")

    # Extract the first_time values in the sorted order
    first_times = query_data["first_time"].values

    # Plot bars for this query at positions offset by i*width
    ax.bar(x + i * width, first_times, width, label=f"Query {q}", color=colors[i % len(colors)])

# Set the x-axis tick positions so they fall in the middle of each group
ax.set_xticks(x + width * (len(queries) - 1) / 2)
ax.set_xticklabels(order)

# Labeling and title
ax.set_xlabel("Dataset Size")
ax.set_ylabel("First Execution Time (ms)")
ax.set_title(f"First Execution Times for {database}")

# Show legend (queries)
ax.legend()

# Adjust layout to prevent label overlap
plt.tight_layout()
plt.show()
