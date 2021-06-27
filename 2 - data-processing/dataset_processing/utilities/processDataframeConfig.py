import os, sys, json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from dataset_processing.utilities.miscProcess import GenerateLogs

SCRIPT_NAME = os.path.basename(__file__)


class DataframeConfig:
    """
    This custom class process dataframe config from a json file and builds schema, input and output path, target columns etc

    """

    def __init__(self, spark):
        self.spark = spark

    def global_parameters(self, temp_path):
        global g_temp_path
        g_temp_path = temp_path

    """ Function to read the json configuration file """

    def json_reader(self, json_file):
        with open(json_file) as jfile:
            df_dict = json.load(jfile)
        return df_dict

    """ Function to get the dataframe partition column """

    def partition_columns(self, df_dict):

        part_col1 = df_dict["targetDataframeDetails"]["dataframePartition1"]
        part_col2 = df_dict["targetDataframeDetails"]["dataframePartition2"]
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_info(SCRIPT_NAME, "Partition Column : {}".format(part_col1))

        return part_col1, part_col2

    """ Function to count the number of columns """

    def count_columns(self, df_dict):
        count = 0
        for x in df_dict:
            if isinstance(df_dict[x], list):
                count += len(df_dict[x])

        print(count)
        return count

    """ Function to build the dataframe column list from the json file"""

    def build_dataframe_column_list(self, df_dict):

        column_list = []
        gen_logs = GenerateLogs(self.spark)

        column_count = len(df_dict["targetDataframeDetails"]["dataframeColumnInfo"])

        for i in range(0, column_count):
            column_list.append(df_dict["targetDataframeDetails"]["dataframeColumnInfo"][i]["columnName"].lower())

        gen_logs.log_info(SCRIPT_NAME, "Dataframe Column List: {}".format(column_list))
        return column_list

    """ Function to build the dataframe schema using the input from json file"""

    def get_dataframe_schema(self, df_dict):

        dtypes = {"IntegerType()": IntegerType(), "StringType()": StringType(), "DoubleType()": DoubleType()}

        cust_schema = StructType()

        column_count = len(df_dict["sources"]["driverSource"]["fields"])

        print(column_count)

        for i in range(0, column_count):
            cust_schema.add(
                df_dict["sources"]["driverSource"]["fields"][i]["name"],
                dtypes[df_dict["sources"]["driverSource"]["fields"][i]["type"]],
                True,
            )

        return cust_schema

    def get_historic_dataframe_schema(self, df_dict):

        dtypes = {"IntegerType()": IntegerType(), "StringType()": StringType(), "DoubleType()": DoubleType()}

        cust_schema = StructType()

        column_count = len(df_dict["sources"]["driverSource"]["historic_fields"])

        print(column_count)

        for i in range(0, column_count):
            cust_schema.add(
                df_dict["sources"]["driverSource"]["historic_fields"][i]["name"],
                dtypes[df_dict["sources"]["driverSource"]["historic_fields"][i]["type"]],
                True,
            )

        return cust_schema

    """ Function to get the dataframe input path """

    def get_source_driverFilerPath(self, df_dict):
        driverFilePath = df_dict["sources"]["driverSource"]["filePath"]
        gen_logs = GenerateLogs(self.spark)

        gen_logs.log_info(SCRIPT_NAME, "driverFilePath: {}".format(driverFilePath))
        return driverFilePath

    """ Function to get the dataframe output path """

    def get_source_OutputPath(self, df_dict):
        outputFilePath = df_dict["sources"]["driverSource"]["OutputPath"]
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_info(SCRIPT_NAME, "OutputPathFilePath: {}".format(outputFilePath))
        return outputFilePath

    """ Function to get the dimension output path """

    def get_source_dateDimOutputPath(self, df_dict):
        datedimOutputPath = df_dict["sources"]["driverSource"]["DimOutputPath"]
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_info(SCRIPT_NAME, "Date Dim OutputPathFilePath: {}".format(datedimOutputPath))
        return datedimOutputPath

    def main(self, input_file):
        print(input_file)
        df_dict = self.json_reader(input_file)
        print(df_dict)
        print(self.load_freq(df_dict))


if __name__ == "__main__":
    DataframeConfig.main(sys.argv[1])
