import findspark

findspark.init()

findspark.find()

import pyspark
import logging
import sys
from operator import add
import pytest
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
from chispa.column_comparer import assert_column_equality
from pathlib import Path
import json
import os
import shutil

from dataset_processing.blockface_processing.executeBlockface import BlockfaceProcessing
from dataset_processing.utilities.processDataframeConfig import DataframeConfig
from dataset_processing.utilities.miscProcess import GenerateLogs
from dataset_processing.occupancy_processing.executeOccupancyProcess import OccupancyProcessing


SCRIPT_NAME = os.path.basename(__file__)

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
logging.info(path)


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """Fixture for creating a spark context."""

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .enableHiveSupport()
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()
    return spark


pytestmark = pytest.mark.usefixtures("spark_session")


def test_blockface_read_csv(mocker, spark_session):
    def mock_load():
        columns = [
            "objectid",
            "station_id",
            "segkey",
            "unitid",
            "unitid2",
            "station_address",
            "side",
            "block_id",
            "block_nbr",
            "csm",
            "parking_category",
            "load",
            "zone",
            "total_zones",
            "wkd_rate1",
            "wkd_start1",
            "wkd_end1",
            "wkd_rate2",
            "wkd_start2",
            "wkd_end2",
            "wkd_rate3",
            "wkd_start3",
            "wkd_end3",
            "sat_rate1",
            "sat_start1",
            "sat_end1",
            "sat_rate2",
            "sat_start2",
            "sat_end2",
            "sat_rate3",
            "sat_start3",
            "sat_end3",
            "rpz_zone",
            "rpz_area",
            "paidarea",
            "parking_time_limit",
            "subarea",
            "start_time_wkd",
            "end_time_wkd",
            "start_time_sat",
            "end_time_sat",
            "primarydistrictcd",
            "secondarydistrictcd",
            "overrideyn",
            "overridecomment",
            "shape_length",
        ]
        data = [
            (
                "1065",
                "79025",
                "11078",
                "7900",
                "90",
                "JOHN ST BETWEEN 9TH AVE N AND WESTLAKE AVE N",
                "N",
                "N2-09",
                "900",
                "Y",
                "Paid Parking",
                "1",
                "1",
                "2",
                "1",
                "480",
                "659",
                "0.5",
                "660",
                "1079",
                "",
                "",
                "",
                "1",
                "480",
                "659",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "South",
                "08AM",
                "06PM",
                "08AM",
                "06PM",
                "DISTRICT7",
                "",
                "N",
                "",
                "",
            )
        ]

        rdd = spark_session.sparkContext.parallelize(data)

        df = spark_session.createDataFrame(rdd).toDF(*columns)
        return df

    mocker.patch(
        "dataset_processing.blockface_processing.executeBlockface.BlockfaceProcessing.sourceBlockfaceReadParquet",
        mock_load,
    )

    actual_df = BlockfaceProcessing.sourceBlockfaceReadParquet()
    # actual_df.printSchema()
    no_of_cols = len(actual_df.columns)

    print(no_of_cols)

    assert no_of_cols == 46


def test_blockface_transformations(spark_session):
    data2 = [
        (
            1065,
            79025,
            11078,
            7900,
            90,
            "JOHN ST BETWEEN 9TH AVE N AND WESTLAKE AVE N",
            "N",
            "N2-09",
            900,
            "Y",
            "Paid Parking",
            1,
            1,
            2,
            1.0,
            480,
            659,
            0.5,
            660,
            1079,
            1.0,
            1080,
            1499,
            1.0,
            480,
            659,
            0.5,
            660,
            1079,
            1.0,
            1080,
            1499,
            24,
            "",
            "South Lake",
            120,
            "South",
            "08AM",
            "06PM",
            "08AM",
            "06PM",
            "DISTRICT7",
            "",
            "N",
            "",
            380.306908791687,
        ),
        (
            1491,
            56353,
            11076,
            7900,
            75,
            "JOHN ST BETWEEN DEXTER AVE N AND 8TH AVE N",
            "N",
            "N2-07",
            900,
            "Y",
            "Paid Parking",
            1,
            1,
            2,
            1.0,
            480,
            659,
            0.5,
            660,
            1079,
            1.0,
            1080,
            1499,
            1.0,
            480,
            659,
            0.5,
            660,
            1079,
            1.0,
            1080,
            1499,
            24,
            "",
            "South Lake",
            120,
            "South",
            "08AM",
            "06PM",
            "08AM",
            "06PM",
            "DISTRICT7",
            "",
            "N",
            "",
            268.306908791687,
        ),
    ]

    gen_logs = GenerateLogs(spark_session)
    gen_logs.initial_log_file("test_log.log")
    dataframeconfig = DataframeConfig(spark_session)
    blockface_config_dict = dataframeconfig.json_reader(
        "C:\\Datasetprocessing\\dataset_processing\\data\\blockface.json"
    )
    # Get Target Table Schema
    TargetDataframeSchema = dataframeconfig.get_dataframe_schema(blockface_config_dict)

    blockface_pr = BlockfaceProcessing(spark_session)
    # rdd = spark_session.sparkContext.parallelize(data)

    df = spark_session.createDataFrame(data=data2, schema=TargetDataframeSchema)

    cols = [
        "station_id",
        "station_address",
        "side",
        "block_nbr",
        "parking_category",
        "wkd_rate1",
        "wkd_start1",
        "wkd_end1",
        "wkd_rate2",
        "wkd_start2",
        "wkd_end2",
        "wkd_rate3",
        "wkd_start3",
        "wkd_end3",
        "sat_rate1",
        "sat_start1",
        "sat_end1",
        "sat_rate2",
        "sat_start2",
        "sat_end2",
        "sat_rate3",
        "sat_start3",
        "sat_end3",
        "parking_time_limit",
        "subarea",
    ]

    blockface_pr.executeBlockfaceOperations(
        src_df=df, output="./tests/", cols_list=cols, max_retry_count=1, retry_delay=3
    )

    actual_df = spark_session.read.parquet("./tests/*.parquet")
    actual_df.show(truncate=False)

    actual_wkd_start_data_list = actual_df.select("wkd_start1").collect()

    actual_wkd_end_data_list = actual_df.select("wkd_end1").collect()

    actual_wkd_start1_array = [str(row["wkd_start1"]) for row in actual_wkd_start_data_list]

    actual_wkd_end1_array = [str(row["wkd_end1"]) for row in actual_wkd_end_data_list]

    assert actual_wkd_start1_array[0] == "08:00:00"
    assert actual_wkd_end1_array[0] == "10:59:00"

    # Removing folders
    shutil.rmtree("./tests/Blockface.parquet/")


def test_occupancy_transformations(spark_session):
    data2 = [
        (
            "04/14/2021 04:26:00 PM",
            4,
            "WOODLAWN AVE NE BETWEEN NE 72ND ST AND NE 73RD ST",
            "SW",
            "59,013",
            120,
            6,
            "Green Lake",
            "",
            0.00,
            "Paid Parking",
            "POINT (-122.32498613 47.6808223)",
        ),
        (
            "04/15/2021 04:26:00 PM",
            4,
            "WOODLAWN AVE NE BETWEEN NE 72ND ST AND NE 73RD ST",
            "SW",
            "89,013",
            120,
            6,
            "Green Lake",
            "",
            0.00,
            "Paid Parking",
            "POINT (-122.33297326 47.59872593)",
        ),
    ]

    gen_logs = GenerateLogs(spark_session)
    gen_logs.initial_log_file("test_log.log")
    dataframeconfig = DataframeConfig(spark_session)
    occupancy_config_dict = dataframeconfig.json_reader(
        "C:\\Datasetprocessing\\dataset_processing\\data\\occupancy.json"
    )
    # Get Target Table Schema
    TargetDataframeSchema = dataframeconfig.get_dataframe_schema(occupancy_config_dict)

    occupancy_pr = OccupancyProcessing(spark_session)
    # rdd = spark_session.sparkContext.parallelize(data)

    occ_df = spark_session.createDataFrame(data=data2, schema=TargetDataframeSchema)

    cols = [
        "occupancydatetime",
        "paidoccupancy",
        "blockfacename",
        "sideofstreet",
        "station_id",
        "pakingtimelimitcategory",
        "available_spots",
        "paidparkingarea",
        "paidparkingsubarea",
        "paidparkingrate",
        "parkingcategory",
        "location",
    ]

    occupancy_pr.executeOccupancyOperations(
        src_df=occ_df,
        output="./tests/",
        datedimoutputpath="./tests/",
        cols_list=cols,
        partn_col="MONTH",
        max_retry_count=1,
        retry_delay=3,
    )

    actual_occ_df = spark_session.read.parquet("./tests/MONTH=April/*.parquet")
    actual_occ_df.show(truncate=False)

    actual_lat_list = actual_occ_df.select("latitude").collect()

    actual_long_list = actual_occ_df.select("longitude").collect()

    actual_lat_array = [str(row["latitude"]) for row in actual_lat_list]

    actual_long_array = [str(row["longitude"]) for row in actual_long_list]

    assert actual_lat_array[0] == "47.6808223"
    assert actual_long_array[0] == "-122.32498613"

    # Removing folders
    shutil.rmtree("./tests/MONTH=April/")


def test_historic_occupancy(spark_session):
    def create_station_id_lookup(spark_session):

        data2 = [
            (
                "05/14/2021 04:26:00 PM",
                4,
                "WOODLAWN AVE NE BETWEEN NE 72ND ST AND NE 73RD ST",
                "SW",
                "59,013",
                120,
                6,
                "Green Lake",
                "",
                0.00,
                "Paid Parking",
                "POINT (-122.32498613 47.6808223)",
            ),
            (
                "05/15/2021 04:26:00 PM",
                4,
                "WOODLAWN AVE NE BETWEEN NE 72ND ST AND NE 73RD ST",
                "SW",
                "89,013",
                120,
                6,
                "Green Lake",
                "",
                0.00,
                "Paid Parking",
                "POINT (-122.33297326 47.59872593)",
            ),
        ]

        gen_logs = GenerateLogs(spark_session)
        gen_logs.initial_log_file("test_log.log")
        dataframeconfig = DataframeConfig(spark_session)
        occupancy_config_dict = dataframeconfig.json_reader(
            "C:\\Datasetprocessing\\dataset_processing\\data\\occupancy.json"
        )
        # Get Target Table Schema
        TargetDataframeSchema = dataframeconfig.get_dataframe_schema(occupancy_config_dict)

        occ_df = spark_session.createDataFrame(data=data2, schema=TargetDataframeSchema)

        cols = [
            "occupancydatetime",
            "paidoccupancy",
            "blockfacename",
            "sideofstreet",
            "station_id",
            "pakingtimelimitcategory",
            "available_spots",
            "paidparkingarea",
            "paidparkingsubarea",
            "paidparkingrate",
            "parkingcategory",
            "location",
        ]

        occ_df.coalesce(1).write.format("csv").save("./tests/station_id_lookup.csv", header="true")

    create_station_id_lookup(spark_session)

    hist_data2 = [
        (
            "05/14/2017 04:26:00 PM",
            4,
            "WOODLAWN AVE NE BETWEEN NE 72ND ST AND NE 73RD ST",
            "SW",
            "59013",
            120,
            6,
            "Green Lake",
            "",
            0.00,
            "Paid Parking",
        ),
        (
            "05/15/2021 04:26:00 PM",
            4,
            "WOODLAWN AVE NE BETWEEN NE 72ND ST AND NE 73RD ST",
            "SW",
            "89013",
            120,
            6,
            "Green Lake",
            "",
            0.00,
            "Paid Parking",
        ),
    ]

    occupancy_pr = OccupancyProcessing(spark_session)
    gen_logs = GenerateLogs(spark_session)
    gen_logs.initial_log_file("test_log.log")
    dataframeconfig = DataframeConfig(spark_session)
    occupancy_config_dict = dataframeconfig.json_reader(
        "C:\\Datasetprocessing\\dataset_processing\\data\\occupancy.json"
    )
    # Get Target Table Schema
    TargetHistOccpDFSchema = dataframeconfig.get_historic_dataframe_schema(occupancy_config_dict)
    TargetDataframeSchema = dataframeconfig.get_dataframe_schema(occupancy_config_dict)

    hist_occ_df = spark_session.createDataFrame(data=hist_data2, schema=TargetHistOccpDFSchema)

    occupancy_pr.executeHistoricOccupancyOperations(
        hist_occ_df, "./tests/", "./tests/station_id_lookup.csv/*.csv", "MONTH", 1, 3, TargetDataframeSchema
    )

    actual_occ_df = spark_session.read.parquet("./tests/MONTH=MAY/*.parquet")
    actual_occ_df.show(truncate=False)

    actual_lat_list = actual_occ_df.select("latitude").collect()

    actual_long_list = actual_occ_df.select("longitude").collect()

    actual_lat_array = [str(row["latitude"]) for row in actual_lat_list]

    actual_long_array = [str(row["longitude"]) for row in actual_long_list]

    assert actual_lat_array[0] == "47.6808223"
    assert actual_long_array[0] == "-122.32498613"

    # rdd = spark_session.sparkContext.parallelize(data)

    # Removing folders
    shutil.rmtree("./tests/MONTH=May/")
    # Removing folders
    shutil.rmtree("./tests/station_id_lookup.csv/")


def test_remove_non_word_characters(spark_session):

    occ_pr = OccupancyProcessing(spark_session)

    data = [("jo&&se", "jose"), ("**li**", "li"), ("77,990", "77990"), (None, None)]
    df = spark_session.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", occ_pr.remove_non_word_characters(F.col("name"))
    )
    assert_column_equality(df, "clean_name", "expected_name")


def test_column_list(spark_session):

    gen_logs = GenerateLogs(spark_session)
    gen_logs.initial_log_file("test_log.log")
    dataframeconfig = DataframeConfig(spark_session)

    json_file = "C://Datasetprocessing//tests//test1.json"

    with open(json_file) as jfile:
        df_dict = json.load(jfile)

    actual_list = dataframeconfig.build_dataframe_column_list(df_dict)

    expected_list = ["occupancydatetime", "occupied_spots"]
    print(actual_list)

    assert actual_list[0] == expected_list[0]
    assert actual_list[1] == expected_list[1]


pytestmark = pytest.mark.usefixtures("spark_session")


def test_file_path(spark_session):
    gen_logs = GenerateLogs(spark_session)
    gen_logs.initial_log_file("test_log.log")
    dataframeconfig = DataframeConfig(spark_session)

    json_file = "C://Datasetprocessing//tests//test1.json"

    with open(json_file) as jfile:
        df_dict = json.load(jfile)

    actual_filepath = dataframeconfig.get_source_driverFilerPath(df_dict)

    expected_filePath = "C:\\Test\\Paid_Parking.csv"

    assert actual_filepath == expected_filePath


pytestmark = pytest.mark.usefixtures("spark_session")


def test_dataframe_partition(spark_session):

    gen_logs = GenerateLogs(spark_session)
    gen_logs.initial_log_file("test_log.log")
    dataframeconfig = DataframeConfig(spark_session)

    json_file = "C://Datasetprocessing//tests//test1.json"

    with open(json_file) as jfile:
        df_dict = json.load(jfile)

    actual_partition = dataframeconfig.partition_column(df_dict)

    expected_partition = "MONTH"

    assert actual_partition == expected_partition


pytestmark = pytest.mark.usefixtures("spark_session")


def test_remove_parenthesis_characters(spark_session):

    occ_pr = OccupancyProcessing(spark_session)

    data = [("(123.88", "123.88"), ("6788.9)", "6788.9")]
    df = spark_session.createDataFrame(data, ["name", "expected_name"]).withColumn(
        "clean_name", occ_pr.remove__parenthesis((F.col("name")))
    )

    actual_data_list = df.select("clean_name").collect()

    actual_data_array = [float(row["clean_name"]) for row in actual_data_list]

    expected_data_list = df.select("expected_name").collect()

    expected_data_array = [float(row["expected_name"]) for row in expected_data_list]

    assert actual_data_array[0] == expected_data_array[0]
    assert actual_data_array[1] == expected_data_array[1]


pytestmark = pytest.mark.usefixtures("spark_session")


def test_date_format(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    occ_pr = OccupancyProcessing(spark_session)

    data = [
        ("04/15/2021 02:33:00 PM", "April"),
        ("05/17/2021 08:01:00 AM", "May"),
    ]
    df = spark_session.createDataFrame(data, ["Datetime", "expected_date_fmt_value"])

    df = df.withColumn("Datetime", occ_pr.timestamp_format(F.col("Datetime"), "MM/dd/yyyy hh:mm:ss a"))

    df = df.withColumn("actual_month", occ_pr.date__format(F.col("Datetime"), "MMMM"))

    actual_month_data_list = df.select("actual_month").collect()

    actual_month_data_array = [str(row["actual_month"]) for row in actual_month_data_list]

    expected_month_data_list = df.select("expected_date_fmt_value").collect()

    expected_month_data_array = [str(row["expected_date_fmt_value"]) for row in expected_month_data_list]

    assert actual_month_data_array[0] == expected_month_data_array[0]
    assert actual_month_data_array[1] == expected_month_data_array[1]


pytestmark = pytest.mark.usefixtures("spark_session")


def test_read_blockface(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    blockface_pr = BlockfaceProcessing(spark_session)
    dataframeconfig = DataframeConfig(spark_session)

    blockface_config_dict = dataframeconfig.json_reader(
        "C:\\Datasetprocessing\\dataset_processing\\data\\blockface.json"
    )

    blockfacefilePath = dataframeconfig.get_source_driverFilerPath(blockface_config_dict)

    # Get Target Table Schema
    TargetDataframeSchema = dataframeconfig.get_dataframe_schema(blockface_config_dict)

    (blockface, source_data_info) = blockface_pr.sourceBlockfaceReadParquet(blockfacefilePath, TargetDataframeSchema)

    blockface.printSchema()

    blockface.head(4)


def test_read_occupancy(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    occ_pr = OccupancyProcessing(spark_session)
    dataframeconfig = DataframeConfig(spark_session)
    gen_logs = GenerateLogs(spark_session)

    occ_config_dict = dataframeconfig.json_reader("C:\\Datasetprocessing\\dataset_processing\\data\\occupancy.json")

    occfilePath = dataframeconfig.get_source_driverFilerPath(occ_config_dict)

    # Get Target Table Schema
    TargetDataframeSchema = dataframeconfig.get_dataframe_schema(occ_config_dict)

    (occupancy, source_data_info) = occ_pr.sourceOccupancyReadParquet(occfilePath, TargetDataframeSchema, "MONTH")

    occupancy.printSchema()

    occupancy.head(4)


def test_historic_read_occupancy(spark_session):
    spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    occ_pr = OccupancyProcessing(spark_session)
    dataframeconfig = DataframeConfig(spark_session)
    gen_logs = GenerateLogs(spark_session)

    occ_config_dict = dataframeconfig.json_reader("C:\\Datasetprocessing\\dataset_processing\\data\\occupancy.json")

    occfilePath = dataframeconfig.get_source_driverFilerPath(occ_config_dict)

    # Get Target Table Schema
    TargetDataframeSchema = dataframeconfig.get_historic_dataframe_schema(occ_config_dict)
    import glob

    file_names = glob.glob(occfilePath)

    for file in file_names:
        year = file.split("\\")[3][:4]
        if year == 2014:
            (occupancy, source_data_info) = occ_pr.sourceOccupancyReadParquet(
                occfilePath, TargetDataframeSchema, "MONTH"
            )
            occupancy.printSchema()
            occupancy.head(4)
            break

    # Removing unwanted files
    if os.path.isfile("./tests/._SUCCESS.crc") and os.path.isfile("./tests/_SUCCESS"):
        os.remove("./tests/._SUCCESS.crc")
        os.remove("./tests/_SUCCESS")
