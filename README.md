# PySpark Data Processing

## Overview

In this task you will learn the basics of PySpark by performing an analysis on the [UCI Wine Quality](https://archive.ics.uci.edu/dataset/186/wine+quality) dataset using RDD operations.

Your task is to transform and analyze the dataset to generate insights into the factors affecting wine quality.

### Wine Quality dataset features

The dataset consists of several features that describe the chemical properties of red and white wines. The `quality` column is a score between 0 and 10 indicating the wine's quality.

Below is a table describing the features in the dataset, including the feature name, the variable name as it appears in the dataset, and the data type.

| __Feature__              | __Name of Variable__     | __Data Type__ |
|--------------------------|--------------------------|---------------|
| Fixed Acidity            | `fixed acidity`          | Float         |
| Volatile Acidity         | `volatile acidity`       | Float         |
| Citric Acid              | `citric acid`            | Float         |
| Residual Sugar           | `residual sugar`         | Float         |
| Chlorides                | `chlorides`              | Float         |
| Free Sulfur Dioxide      | `free sulfur dioxide`    | Float         |
| Total Sulfur Dioxide     | `total sulfur dioxide`   | Float         |
| Density                  | `density`                | Float         |
| pH Level                 | `pH`                     | Float         |
| Sulphates                | `sulphates`              | Float         |
| Alcohol Content          | `alcohol`                | Float         |
| __Quality Score__        | `quality`                | Integer       |
| __Type__ _(Red or White)_| `type`                   | Categorical   |

__Notes:__

- __Fixed Acidity:__ Concentration of non-volatile acids (e.g., tartaric acid) in g/dm³.
- __Volatile Acidity:__ Concentration of volatile acids (e.g., acetic acid) in g/dm³.
- __Citric Acid:__ Concentration of citric acid in g/dm³.
- __Residual Sugar:__ Amount of sugar remaining after fermentation in g/dm³.
- __Chlorides:__ Concentration of chlorides (salt) in g/dm³.
- __Free Sulfur Dioxide:__ Amount of free SO₂ in mg/dm³.
- __Total Sulfur Dioxide:__ Total amount of SO₂ in mg/dm³.
- __Density:__ Density of the wine in g/cm³.
- __pH Level:__ Measure of acidity or basicity.
- __Sulphates:__ Concentration of potassium sulphate in g/dm³.
- __Alcohol Content:__ Alcohol percentage by volume (%).
- __Quality Score:__ Wine quality score (0 to 10) given by expert tasters.
- __Type:__ Indicates the type of wine (`'red'` or `'white'`). This feature is not directly specified in the data, but rather in the filename.


### Project structure

The project structure is as follows:

```
.
├── README.md
├── input                       # Input data
│   ├── winequality-red.csv
│   └── winequality-white.csv
├── main.py                     # Main file to launch for convenience
├── requirements.txt
├── src
│   ├── __init__.py
│   ├── logger.py
│   ├── spark.py
│   └── tasks.py                # Files with tasks to solve
└── tests
    └── test_tasks.py
```

## Setup

Before performing the task, you need to set up the development environment.

### Creating a Python virtual environment

1. For Windows:
    ```powershell
    python -m venv venv
    ```

2. For Linux/macOS:
    ```bash
    python3 -m venv venv
    ```

### Activating the virtual environment

1. For Windows:
    ```powershell
    .\venv\Scripts\activate
    ```

2. For Linux/macOS:
    ```bash
    source venv/bin/activate
    ```

> [!NOTE]
> After activating the virtual environment, you will see the `(venv)` prefix in front of the command line.

### Installing the required packages from the `requirements.txt` file

1. For both systems:
    ```bash
    pip install -r requirements.txt
    ```

## Tasks

Tasks are located in `src/tasks.py`.

> [!IMPORTANT]  
> Check the `main.py` file to understand how all the functions are used. It will help you with the correct implementation.

### Load and clean the datasets

Load `winequality-red.csv` and `winequality-white.csv` as separate RDDs by implementing `load_dataset(sc: SparkContext, file_path: str, wine_type: str) -> RDD` function. The datasets are loaded as a RDD of strings.

Remove the header row from the RDD (first element) by filtering it out.

Split the strings using a separator in the file. You will receive a RDD with each element being a `list` of features.

Add `wine_type` as a last value to every list of features.

> [!NOTE]
> The datasets are joined in the `main.py` file, not need to join them yourself.

### Filter by `quality`

Implement `filter_wines_by_quality(data_rdd: RDD, quality_threshold: int, comparator: Literal["gte", "lte"]) -> RDD` function
to create separate RDDs for:
- High-quality wines (`quality >= 7`)
- Low-quality wines (`quality <= 4`)

> [!TIP]
> We don't know column names, but you can check the file header row and see what index `quality` has in the list of features of each RDD.

### Analyze `alcohol` content

Compute the average alcohol content for:
- High-quality wines
- Low-quality wines

To do so, implement `compute_average_feature(data_rdd: RDD, feature_index: int) -> float` function.

Inside, you must first `map` the required 'column' from the list.

And them apply `aggregate` transformation by implementing to nested functions for per-partition average (`partOp`) and total average (`totalOp`).

### Distribution of `quality` scores

Count the number of wines for each quality score across the entire dataset by implementing the `quality_distribution(data_rdd: RDD) -> RDD` function.

You need to follow the classic 'word count' approach by mapping the original RDD to a key-value pair (`tuple`) RDD with `quality` as a key.

Then, reduce by key to calculate the quantities of wine in each quality 'category'.

> [!TIP]
> We don't know column names, but you can check the file header row and see what index `quality` has in the list of features of each RDD.

### Analysis by `wine_type`

Calculate the average `alcohol` content for red and white wines separately by implementing `compute_analysis_by_type(data_rdd: RDD) -> RDD` function.

It follows the same approch as in quality distribution, but we need to go deeper :).

First, map to a key-value pair (`tuple`) RDD with each element having `wine_type` as key and each value also a 'counting' tuple.

Then you need to reduce by key to process the nested tuple and return a new tuple with count and sum of alcohol per type.

Finally, map the values only to compute the average.

### Save results

Save the following analyses as text files:
- Distribution of quality scores (`output/quality_distribution`)
- Average alcohol content by wine type (`output/type_analysis`)

To do this, implement `save_rdd_to_file(rdd: RDD, output_path: str) -> None` function and inside, save the required RDDs in a `"key: value"` format, e.g., for alcohol by type:

```
red: 5.5
white: 1.8
```
