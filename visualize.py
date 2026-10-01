import os
import matplotlib.pyplot as plt
from src.spark import start_spark
from src.tasks import (load_dataset, filter_wines_by_quality,
                       compute_average_feature, quality_distribution,
                       compute_analysis_by_type)

if __name__ == "__main__":
    sc, logger = start_spark(number_cores=2, memory_gb=1)

    red_wine_rdd = load_dataset(sc, "input/winequality-red.csv", "red")
    white_wine_rdd = load_dataset(sc, "input/winequality-white.csv", "white")
    data_rdd = red_wine_rdd.union(white_wine_rdd)

    high_quality_rdd = filter_wines_by_quality(data_rdd, 7, "gte")
    low_quality_rdd = filter_wines_by_quality(data_rdd, 4, "lte")
    high_avg = compute_average_feature(high_quality_rdd, 10)
    low_avg = compute_average_feature(low_quality_rdd, 10)

    quality_counts = sorted(quality_distribution(data_rdd).collect())
    type_averages = sorted(compute_analysis_by_type(data_rdd).collect())

    red_dist = sorted(quality_distribution(red_wine_rdd).collect())
    white_dist = sorted(quality_distribution(white_wine_rdd).collect())

    sc.stop()

    os.makedirs("img", exist_ok=True)

    scores = []
    counts = []
    for score, count in quality_counts:
        scores.append(str(score))
        counts.append(count)

    plt.figure(figsize=(8, 5))
    bars = plt.bar(scores, counts, color="slateblue")
    plt.bar_label(bars)
    plt.title("Distribution of wine quality scores")
    plt.xlabel("Quality score")
    plt.ylabel("Number of wines")
    plt.tight_layout()
    plt.savefig("img/quality_distribution.png")
    plt.close()

    types = []
    averages = []
    for wine_type, average in type_averages:
        types.append(wine_type)
        averages.append(round(average, 2))

    plt.figure(figsize=(6, 5))
    bars = plt.bar(types, averages, color=["darkred", "goldenrod"])
    plt.bar_label(bars)
    plt.title("Average alcohol content by wine type")
    plt.xlabel("Wine type")
    plt.ylabel("Alcohol, %")
    plt.ylim(0, 12)
    plt.tight_layout()
    plt.savefig("img/alcohol_by_type.png")
    plt.close()

    plt.figure(figsize=(6, 5))
    bars = plt.bar(["Low quality (<= 4)", "High quality (>= 7)"],
                   [round(low_avg, 2), round(high_avg, 2)],
                   color=["gray", "seagreen"])
    plt.bar_label(bars)
    plt.title("Average alcohol content: low vs high quality")
    plt.ylabel("Alcohol, %")
    plt.ylim(0, 13)
    plt.tight_layout()
    plt.savefig("img/alcohol_by_quality.png")
    plt.close()

    all_scores = []
    for score, count in red_dist:
        if score not in all_scores:
            all_scores.append(score)
    for score, count in white_dist:
        if score not in all_scores:
            all_scores.append(score)
    all_scores.sort()

    red_counts = []
    white_counts = []
    for score in all_scores:
        red_value = 0
        for s, c in red_dist:
            if s == score:
                red_value = c
        white_value = 0
        for s, c in white_dist:
            if s == score:
                white_value = c
        red_counts.append(red_value)
        white_counts.append(white_value)

    positions = []
    for i in range(len(all_scores)):
        positions.append(i)
    red_positions = []
    white_positions = []
    for p in positions:
        red_positions.append(p - 0.2)
        white_positions.append(p + 0.2)

    plt.figure(figsize=(9, 5))
    plt.bar(red_positions, red_counts, width=0.4, color="darkred", label="red")
    plt.bar(white_positions, white_counts, width=0.4, color="goldenrod",
            label="white")
    plt.xticks(positions, all_scores)
    plt.title("Quality scores by wine type")
    plt.xlabel("Quality score")
    plt.ylabel("Number of wines")
    plt.legend()
    plt.tight_layout()
    plt.savefig("img/quality_by_type.png")
    plt.close()
