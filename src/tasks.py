"""
This module contains the Spark tasks.
"""

from pyspark import SparkContext, RDD
from typing import Literal


def load_dataset(sc: SparkContext, file_path: str, wine_type: str) -> RDD:
    """
    Load the dataset, clean it, and add wine type as an additional column.
    
    Args:
        sc (SparkContext): The SparkContext for distributed computation.
        file_path (str): Path to the dataset file (CSV format).
        wine_type (str): The type of wine (e.g., "red" or "white").
        
    Returns:
        RDD: An RDD where each row is a list representing a wine record, 
        with the wine type appended as the last element.
    
    Process:
        - Read the dataset from the given file path.
        - Remove the header row to avoid processing metadata.
        - Split each row into a list of values using the delimiter.
        - Append the wine type (red/white) to each record.
    """

    lines_rdd = sc.textFile(file_path)
    header = lines_rdd.first()

    if ";" in header:
        separator = ";"
    else:
        separator = ","

    def parse_line(line):
        values = line.split(separator)
        values.append(wine_type)
        return values

    return lines_rdd.filter(lambda line: line != header).map(parse_line)


def filter_wines_by_quality(data_rdd: RDD, quality_threshold: int,
                            comparator: Literal["gte", "lte"]) -> RDD:
    """
    Filter wines based on a quality threshold and a comparison operator.
    
    Args:
        data_rdd (RDD): Input RDD where each record represents a wine and includes a quality score.
        quality_threshold (int): The quality score threshold for filtering.
        comparator (Literal["gte", "lte"]): The comparison operator:
            - "gte": Include wines with quality greater than or equal to the threshold.
            - "lte": Include wines with quality less than or equal to the threshold.
    
    Returns:
        RDD: Filtered RDD containing only records that meet the specified condition.
    
    Raises:
        ValueError: If an invalid comparator is provided.
    
    Process:
        - Filter records based on the quality score column.
        - Implement "greater than or equal to" and "less than or equal to" comparisons.
        - Throw 'ValueError' if comparator is not a supported value.
    """

    quality_index = 11

    if comparator == "gte":
        return data_rdd.filter(
            lambda row: int(row[quality_index]) >= quality_threshold)
    if comparator == "lte":
        return data_rdd.filter(
            lambda row: int(row[quality_index]) <= quality_threshold)

    raise ValueError(f"Unsupported comparator: {comparator}")


def compute_average_feature(data_rdd: RDD, feature_index: int) -> float:
    """
    Compute the average value of a specified feature.
    
    Args:
        data_rdd (RDD): Input RDD where each record represents a wine.
        feature_index (int): The index of the feature (column) to average.
    
    Returns:
        float: The average value of the specified feature. Returns 0.0 if no records exist.
    
    Process:
        - Extract the specified feature column.
        - Use aggregate operations to calculate the total sum and count of values.
        - Compute the average as the total divided by the count.
    """

    def partOp(acc: tuple[int, float], x: int | float):
        """
        Accumulates the count and sum for a single partition.
        
        Args:
            acc (tuple[int, float]): Current accumulator state:
                - acc[0]: Count of elements.
                - acc[1]: Sum of elements.
            x (float): The value to add to the accumulator.
        
        Returns:
            tuple[int, float]: Updated accumulator state.
        """
        return (acc[0] + 1, acc[1] + x)

    def totalOp(acc1: tuple[int, float], acc2: tuple[int, float]):
        """
        Combines accumulator results from different partitions.
        
        Args:
            acc1 (tuple[int, float]): First accumulator state.
            acc2 (tuple[int, float]): Second accumulator state.
        
        Returns:
            tuple[int, float]: Combined accumulator state.
        """
        return (acc1[0] + acc2[0], acc1[1] + acc2[1])

    feature_rdd = data_rdd.map(lambda row: float(row[feature_index]))
    count, total = feature_rdd.aggregate((0, 0.0), partOp, totalOp)

    if count == 0:
        return 0.0
    return total / count


def quality_distribution(data_rdd: RDD) -> RDD:
    """
    Calculate the distribution of wines by quality score.
    
    Args:
        data_rdd (RDD): Input RDD where each record represents a wine and includes a quality score.
    
    Returns:
        RDD: An RDD of key-value pairs where:
            - Key: Quality score (int).
            - Value: Count of wines with that quality score (int).
    
    Process:
        - Map each record to a (quality, 1) key-value pair.
        - Aggregate counts for each quality score.
    """

    quality_index = 11
    pairs_rdd = data_rdd.map(lambda row: (int(row[quality_index]), 1))
    return pairs_rdd.reduceByKey(lambda a, b: a + b)


def compute_analysis_by_type(data_rdd: RDD) -> RDD:
    """
    Perform an analysis of average alcohol content grouped by wine type.
    
    Args:
        data_rdd (RDD): Input RDD where each record represents a wine and includes the wine type.
    
    Returns:
        RDD: An RDD of key-value pairs where:
            - Key: Wine type (str, e.g., "red" or "white").
            - Value: Average alcohol content (float) for that type.
    
    Process:
        - Map each record to a (wine_type, (1, alcohol_content)) key-value pair.
        - Aggregate counts and total alcohol content for each wine type using.
        - Compute the average alcohol content as total alcohol divided by count for each type.
    """

    alcohol_index = 10
    pairs_rdd = data_rdd.map(lambda row: (row[-1],
                                          (1, float(row[alcohol_index]))))
    totals_rdd = pairs_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return totals_rdd.mapValues(lambda value: value[1] / value[0])


def save_rdd_to_file(rdd: RDD, output_path: str) -> None:
    """
    Save the contents of an RDD to a text file.
    
    Args:
        rdd (RDD): Input RDD containing the data to save.
        output_path (str): Path to the output directory where the file will be saved.
    
    Process:
        - Convert each RDD record into a string representation 'key: value'.
        - Save the records as a text file in the specified output directory.
    """
    lines_rdd = rdd.map(lambda pair: f"{pair[0]}: {pair[1]}")
    lines_rdd.saveAsTextFile(output_path)
