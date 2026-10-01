import pytest
from pyspark import SparkContext
from tasks import (load_dataset, filter_wines_by_quality,
                   compute_average_feature, quality_distribution,
                   compute_analysis_by_type, save_rdd_to_file)


@pytest.fixture(scope="session")
def sc():
    """Create and provide a local SparkContext."""
    sc = SparkContext("local[1]", "PySparkTest")
    yield sc
    sc.stop()


def test_load_dataset(sc, tmp_path):
    data = [
        "fixed acidity,volatile acidity,citric acid,residual sugar,chlorides,free sulfur dioxide,total sulfur dioxide,density,pH,sulphates,alcohol,quality",
        "7.4,0.7,0,1.9,0.076,11,34,0.9978,3.51,0.56,9.4,5",
        "7.8,0.88,0,2.6,0.098,25,67,0.9968,3.2,0.68,10.5,7"
    ]
    file_path = tmp_path / "test_load_dataset" / "test_data.csv"
    sc.parallelize(data).saveAsTextFile(str(file_path.resolve()))
    rdd = load_dataset(sc, str(file_path.resolve()), "red")
    assert rdd.count() == 2
    assert rdd.first()[-1] == "red"


def test_merge_datasets(sc):
    red_data = sc.parallelize([[
        "7.4", "0.7", "0", "1.9", "0.076", "11", "34", "0.9978", "3.51",
        "0.56", "9.4", "5", "red"
    ]])
    white_data = sc.parallelize([[
        "7.8", "0.88", "0", "2.6", "0.098", "25", "67", "0.9968", "3.2",
        "0.68", "10.5", "7", "white"
    ]])
    merged_rdd = red_data.union(white_data)
    assert merged_rdd.count() == 2
    assert set(
        merged_rdd.map(lambda row: row[-1]).collect()) == {"red", "white"}


def test_filter_wines_by_quality(sc):
    data_rdd = sc.parallelize([[
        "7.4", "0.7", "0", "1.9", "0.076", "11", "34", "0.9978", "3.51",
        "0.56", "9.4", "5", "red"
    ],
                               [
                                   "7.8", "0.88", "0", "2.6", "0.098", "25",
                                   "67", "0.9968", "3.2", "0.68", "10.5", "7",
                                   "white"
                               ]])
    high_quality_rdd = filter_wines_by_quality(data_rdd, 7, "gte")
    assert high_quality_rdd.count() == 1
    assert high_quality_rdd.first()[-2] == "7"


def test_compute_average_feature(sc):
    data_rdd = sc.parallelize([[
        "7.4", "0.7", "0", "1.9", "0.076", "11", "34", "0.9978", "3.51",
        "0.56", "9.4", "5", "red"
    ],
                               [
                                   "7.8", "0.88", "0", "2.6", "0.098", "25",
                                   "67", "0.9968", "3.2", "0.68", "10.5", "7",
                                   "white"
                               ]])
    avg_alcohol = compute_average_feature(data_rdd, 10)
    assert pytest.approx(avg_alcohol, 0.01) == 9.95


def test_quality_distribution(sc):
    data_rdd = sc.parallelize([[
        "7.4", "0.7", "0", "1.9", "0.076", "11", "34", "0.9978", "3.51",
        "0.56", "9.4", "5", "red"
    ],
                               [
                                   "7.8", "0.88", "0", "2.6", "0.098", "25",
                                   "67", "0.9968", "3.2", "0.68", "10.5", "7",
                                   "white"
                               ]])
    quality_dist = quality_distribution(data_rdd).collect()
    assert len(quality_dist) == 2
    assert (5, 1) in quality_dist
    assert (7, 1) in quality_dist


def test_compute_analysis_by_type(sc):
    data_rdd = sc.parallelize([[
        "7.4", "0.7", "0", "1.9", "0.076", "11", "34", "0.9978", "3.51",
        "0.56", "9.4", "5", "red"
    ],
                               [
                                   "7.8", "0.88", "0", "2.6", "0.098", "25",
                                   "67", "0.9968", "3.2", "0.68", "10.5", "7",
                                   "white"
                               ],
                               [
                                   "7.4", "0.7", "0", "1.9", "0.076", "11",
                                   "34", "0.9978", "3.51", "0.56", "9.4", "6",
                                   "red"
                               ]])
    type_analysis = compute_analysis_by_type(data_rdd).collect()
    assert len(type_analysis) == 2
    assert any(key == "red" for key, _ in type_analysis)
    assert any(key == "white" for key, _ in type_analysis)


def test_save_rdd_to_file(sc, tmp_path):
    data_rdd = sc.parallelize([(5, 1), (7, 2), (6, 1)])
    output_path = tmp_path / "test_save_rdd_to_file" / "quality_distribution"
    save_rdd_to_file(data_rdd, str(output_path.resolve()))

    output_files = list(output_path.iterdir())
    assert len(output_files) > 0
    assert any(file.stem == "part-00000" for file in output_files)
