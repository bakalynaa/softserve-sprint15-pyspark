import os
import json
import shutil
from src.spark import start_spark
from src.tasks import (load_dataset, filter_wines_by_quality,
                       compute_average_feature, quality_distribution,
                       compute_analysis_by_type, save_rdd_to_file)

if __name__ == "__main__":
    sc, logger = start_spark(number_cores=2, memory_gb=1)

    # Load datasets for red and white wines, adding the wine type
    red_wine_rdd = load_dataset(sc, "input/winequality-red.csv", "red")
    white_wine_rdd = load_dataset(sc, "input/winequality-white.csv", "white")

    # Merge the datasets
    data_rdd = red_wine_rdd.union(white_wine_rdd)

    # Filter high and low-quality wines
    high_quality_rdd = filter_wines_by_quality(data_rdd, 7, "gte")
    low_quality_rdd = filter_wines_by_quality(data_rdd, 4, "lte")

    # Compute average alcohol content
    alcohol_index = 10
    high_quality_avg_alcohol = compute_average_feature(high_quality_rdd,
                                                       alcohol_index)
    low_quality_avg_alcohol = compute_average_feature(low_quality_rdd,
                                                      alcohol_index)
    logger.warn(
        json.dumps({
            "high_quality_avg_alcohol": high_quality_avg_alcohol,
            "low_quality_avg_alcohol": low_quality_avg_alcohol
        }))

    # Compute quality distribution
    quality_dist = quality_distribution(data_rdd)

    # Perform analysis by wine type
    type_analysis = compute_analysis_by_type(data_rdd)

    # Save results to files
    outpath = "output"
    if os.path.exists(outpath) and os.path.isdir(outpath):
        shutil.rmtree(outpath)

    save_rdd_to_file(quality_dist, "output/quality_distribution")
    save_rdd_to_file(type_analysis, "output/type_analysis")

    sc.stop()
