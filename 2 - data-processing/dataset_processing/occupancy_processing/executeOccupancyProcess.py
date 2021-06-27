import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, array_contains, date_format, regexp_replace
from pathlib import Path
from datetime import datetime, timedelta
from pyspark.sql.functions import udf
import os
import time
from dataset_processing.utilities.miscProcess import GenerateLogs

SCRIPT_NAME = os.path.basename(__file__)


class OccupancyProcessing:
    """
    This class works on processing historic and delta datasets for Seattle Paid Parking Occupancy

    """

    def __init__(self, spark):
        self.spark = spark

    def global_SQLContext(self, spark1):
        global spark
        self.spark = spark1

    def global_EffectDt(self, iEffevtDt):
        global EffectvDt
        EffectvDt = datetime.strptime(iEffevtDt, "%Y-%m%d").date()

    def get_currentDate(self):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%Y-%m-%d")
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_print(str_current_time)

    """ Function to remove non word characters e.g. '19,788' -> '19788'"""

    def remove_non_word_characters(self, col):
        return F.regexp_replace(col, "[^\\w\\s]+", "")

    """ Function to remove parenthesis e.g. (108.88 > 108.88 """

    def remove__parenthesis(self, col):
        return F.regexp_replace(col, "\(|\)", "")

    """ Function to convert timestamp column value to a specific date format """

    def date__format(self, col, formattype):
        return F.date_format(col, formattype)

    """ Function to convert a column to a timestampformat """

    def timestamp_format(self, col, timestampformat):
        return F.to_timestamp(col, format=timestampformat)

    """ Read Parquet function - input file path, schema and partition name """

    def sourceOccupancyReadParquet(self, occupancyFilePath, custom_schema, partition_value1, partition_value2):
        gen_logs = GenerateLogs(self.spark)
        gen_logs.log_info(SCRIPT_NAME, "Reading Occupancy CSV file...")
        print("Reading Occupancy CSV file")

        source_data_info = {}
        source_data_info["type"] = "CSV"

        # filepath = source_config['sources']['driverSource']["filePath"]
        print("Occupancy file path : {}".format(occupancyFilePath))

        try:
            occupancy = (
                self.spark.read.format("csv").option("header", True).schema(custom_schema).load(occupancyFilePath)
            )

        except Exception as e:
            gen_logs.log_info(SCRIPT_NAME, "error in reading csv: {}".format(e))

        source_data_info["occupancyFilePath"] = occupancyFilePath
        source_data_info["partition1"] = str(partition_value1)
        source_data_info["partition2"] = str(partition_value2)

        return (occupancy, source_data_info)


    """ Function to transform historic and delta paid parking occupancy datasets (>=2018) """

    def executeOccupancyOperations(
        self, src_df, output, datedimoutputpath, cols_list, partn_col1, partn_col2, max_retry_count, retry_delay
    ):
        today = datetime.now()
        current_year = today.year
        PartitionColumn1 = partn_col1
        PartitionColumn2 = partn_col2

        gen_logs = GenerateLogs(self.spark)

        ReturnCode = 0
        rec_cnt = 0
        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:

            try:
                Success = True
                # reading from DBFS
                input_df = src_df

            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file after {} tries: {}".format(max_retry_count)
                    )
                    ReturnCode = 1
                    return ReturnCode, rec_cnt

                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file, re-try in {} seconds ".format(retry_delay)
                    )

        select_df = input_df.select([colname for colname in input_df.columns])
        # if colname in (cols_list)])

        select_df = select_df.withColumn("station_id", F.col("sourceelementkey"))
        select_df = select_df.drop("sourceelementkey")

        print("Reading inside transformation function")
        
        for x in range(len(cols_list)):
            if cols_list[x] == "sourceelementkey":
                column = 'station_id'

                select_df = select_df.withColumn(column, self.remove_non_word_characters(F.col("station_id")))
                select_df = select_df.withColumn(column, select_df[column].cast(IntegerType()))

            if cols_list[x] == "occupancydatetime":
                column = cols_list[x]

                self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
                select_df = select_df.withColumn(column, self.timestamp_format(F.col(column), "MM/dd/yyyy hh:mm:ss a"))

                select_df = select_df.withColumn(PartitionColumn1, self.date__format(F.col(column), "yyyy"))

                select_df = select_df.withColumn(PartitionColumn2, self.date__format(F.col(column), "MMMM"))

                date_dim = select_df.withColumn("day_of_week", self.date__format(F.col(column), "EEEE"))\
                                    .withColumn("monthname", self.date__format(F.col(column), "MMMM")) \
                                    .withColumn("month", self.date__format(F.col(column), "MMMM")) \
                                    .select("occupancydatetime","day_of_week", "monthname","month", PartitionColumn1)\
                                    .drop_duplicates()
                                    
                                    #.select("occupancydatetime","day_of_week", "month", PartitionColumn1)

                date_dim = date_dim.select("occupancydatetime", "day_of_week","monthname","month",PartitionColumn1)

            if cols_list[x] == "location":

                column = cols_list[x]
                split_col = ["longitude", "latitude"]

                select_df = select_df.withColumn(split_col[0], F.split(column, " ").getItem(1)).withColumn(
                    split_col[1], F.split(column, " ").getItem(2)
                )

                select_df = select_df.withColumn(split_col[0], self.remove__parenthesis(col(split_col[0]))).withColumn(
                    split_col[1], self.remove__parenthesis(col(split_col[1]))
                )

                select_df = select_df.withColumn(split_col[0], select_df[split_col[0]].cast(DoubleType())).withColumn(
                    split_col[1], select_df[split_col[1]].cast(DoubleType())
                )

                select_df = select_df.drop(column)

            #   select_df = select_df.select(cols_list)
            # select_df = select_df.select([colname for colname in input_df.columns if colname in (cols_list)])

        # drop rows having null values in columns
        select_df=select_df.na.drop(subset=["latitude","longitude"])

        select_df=select_df.select('occupancydatetime','paidoccupancy','blockfacename',\
                                'sideofstreet','parkingtimelimitcategory','available_spots',\
                                'paidparkingarea','paidparkingsubarea','paidparkingrate','parkingcategory',\
                                'latitude','longitude','station_id',PartitionColumn1,PartitionColumn2)


        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:
            try:
                Success = True
               # select_df.show(3)
                gen_logs.log_print("Writing occupancy dataframe to output file: {}".format(output))

              
                select_df.write.mode("append").partitionBy(PartitionColumn1, PartitionColumn2).parquet(output)

                gen_logs.log_print("Writing date dimension to output file: {}".format(datedimoutputpath))
               # date_dim.show(3)
                date_dim.write.mode("append").partitionBy(PartitionColumn1, "month").parquet(datedimoutputpath)
            
            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME,
                        "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output),
                    )
                    ReturnCode = 2
                    return ReturnCode, rec_cnt
                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay)
                    )
                    time.sleep(retry_delay)

        gen_logs.log_print("Number of Records Processed: {}".format(rec_cnt))
        return ReturnCode, rec_cnt
    
    def executeDeltaOccupancyOperations(
        self, src_df, output, datedimoutputpath,last_date_processed, cols_list, partn_col1, partn_col2, max_retry_count, retry_delay
    ):
        today = datetime.now()
        current_year = today.year
        PartitionColumn1 = partn_col1
        PartitionColumn2 = partn_col2

        gen_logs = GenerateLogs(self.spark)

        ReturnCode = 0
        rec_cnt = 0
        RetryCt = 0
        Success = False
        processed = False

        while (RetryCt < max_retry_count) and not Success:

            try:
                Success = True
                # reading from DBFS
                input_df = src_df

            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file after {} tries: {}".format(max_retry_count)
                    )
                    ReturnCode = 1
                    return ReturnCode, rec_cnt

                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file, re-try in {} seconds ".format(retry_delay)
                    )

        

        select_df = input_df.select([colname for colname in input_df.columns])
        # if colname in (cols_list)])

        select_df = select_df.withColumn("station_id", F.col("sourceelementkey"))
        select_df = select_df.drop("sourceelementkey")

        print("Reading inside transformation function")
        
        for x in range(len(cols_list)):
            if cols_list[x] == "sourceelementkey":
                column = 'station_id'

                select_df = select_df.withColumn(column, self.remove_non_word_characters(F.col("station_id")))
                select_df = select_df.withColumn(column, select_df[column].cast(IntegerType()))

            if cols_list[x] == "occupancydatetime":
                column = cols_list[x]

                self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
                select_df = select_df.withColumn(column, self.timestamp_format(F.col(column), "MM/dd/yyyy hh:mm:ss a"))

                select_df = select_df.withColumn(PartitionColumn1, self.date__format(F.col(column), "yyyy"))

                select_df = select_df.withColumn(PartitionColumn2, self.date__format(F.col(column), "MMMM"))

                date_dim = select_df.withColumn("day_of_week", self.date__format(F.col(column), "EEEE"))\
                                    .withColumn("monthname", self.date__format(F.col(column), "MMMM")) \
                                    .withColumn("month", self.date__format(F.col(column), "MMMM")) \
                                    .select("occupancydatetime","day_of_week", "monthname","month", PartitionColumn1)\
                                    .drop_duplicates()
                                   
                                    # .select("occupancydatetime","day_of_week", "month", PartitionColumn1)

                date_dim = date_dim.select("occupancydatetime", "day_of_week","monthname","month",PartitionColumn1)

            if cols_list[x] == "location":

                column = cols_list[x]
                split_col = ["longitude", "latitude"]

                select_df = select_df.withColumn(split_col[0], F.split(column, " ").getItem(1)).withColumn(
                    split_col[1], F.split(column, " ").getItem(2)
                )

                select_df = select_df.withColumn(split_col[0], self.remove__parenthesis(col(split_col[0]))).withColumn(
                    split_col[1], self.remove__parenthesis(col(split_col[1]))
                )

                select_df = select_df.withColumn(split_col[0], select_df[split_col[0]].cast(DoubleType())).withColumn(
                    split_col[1], select_df[split_col[1]].cast(DoubleType())
                )

                select_df = select_df.drop(column)

            #   select_df = select_df.select(cols_list)
            # select_df = select_df.select([colname for colname in input_df.columns if colname in (cols_list)])

        select_df=select_df.na.drop(subset=["latitude","longitude"])

        select_df=select_df.select('occupancydatetime','paidoccupancy','blockfacename',\
                                'sideofstreet','parkingtimelimitcategory','available_spots',\
                                'paidparkingarea','paidparkingsubarea','paidparkingrate','parkingcategory',\
                                'latitude','longitude','station_id',PartitionColumn1,PartitionColumn2)


        select_df.registerTempTable("df_table")
        max_date=self.spark.sql("SELECT MAX(occupancydatetime) as maxoccupancydate FROM df_table").first().asDict()['maxoccupancydate']
        
        print("Max date from the list is {}".format(max_date))

        print(last_date_processed)
        if last_date_processed!='No entry':
            select_df =select_df.filter(F.col("occupancydatetime") > (last_date_processed))
            date_dim = date_dim.filter(F.col("occupancydatetime") > (last_date_processed))
        
        count_df= select_df.count()
        print("Record count is {}".format(count_df))

        if count_df > 0:

            RetryCt = 0
            Success = False

            while (RetryCt < max_retry_count) and not Success:
                try:
                    Success = True
                   # select_df.show(3)
                    gen_logs.log_print("Writing occupancy dataframe to output file: {}".format(output))

                    select_df.write.mode("append").partitionBy(PartitionColumn1, PartitionColumn2).parquet(output)

                    gen_logs.log_print("Writing date dimension to output file: {}".format(datedimoutputpath))
                   # date_dim.show(3)
                    date_dim.write.mode("append").partitionBy(PartitionColumn1, "month").parquet(datedimoutputpath)
                            
                except:
                    Success = False
                    RetryCt += 1
                    if RetryCt == max_retry_count:
                        gen_logs.log_info(
                            SCRIPT_NAME,
                            "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output),
                        )
                        ReturnCode = 2
                        return ReturnCode, rec_cnt
                    else:
                        gen_logs.log_info(
                            SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay)
                        )
                        time.sleep(retry_delay)
        else:
            gen_logs.log_info(
                            SCRIPT_NAME, "No additional records to be processed"
                        )
            ReturnCode=0
            processed = True


        return ReturnCode, rec_cnt, max_date, processed


    """ Function to transform historic paid parking datasets from 2012-2017 """

    def executeHistoricOccupancyOperations(
        self, src_df, output, datedimoutputpath, cols_list, partn_col1,partn_col2,max_retry_count, retry_delay
    ):

        PartitionColumn1 = partn_col1
        PartitionColumn2 = partn_col2

        gen_logs = GenerateLogs(self.spark)

        ReturnCode = 0
        rec_cnt = 0
        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:

            try:
                Success = True
                # reading from DBFS
                input_df = src_df

            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file after {} tries: {}".format(max_retry_count)
                    )
                    ReturnCode = 1
                    return ReturnCode, rec_cnt

                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on reading input file, re-try in {} seconds ".format(retry_delay)
                    )

        select_df = input_df.select([colname for colname in input_df.columns])
        # if colname in (cols_list)])

        select_df = select_df.withColumn("station_id", F.col("sourceelementkey"))
        select_df = select_df.drop("sourceelementkey")

        print("Reading inside transformation function")
       
        for x in range(len(cols_list)):
            if cols_list[x] == "sourceelementkey":
                column = 'station_id'
                select_df = select_df.withColumn(column, select_df[column].cast(IntegerType()))

            if cols_list[x] == "occupancydatetime":
                column = cols_list[x]

                self.spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
                select_df = select_df.withColumn(column, self.timestamp_format(F.col(column), "yyyy-MM-dd HH:mm:SS"))

                select_df = select_df.withColumn(PartitionColumn1, self.date__format(F.col(column), "yyyy"))

                select_df = select_df.withColumn(PartitionColumn2, self.date__format(F.col(column), "MMMM"))

                date_dim = select_df.withColumn("day_of_week", self.date__format(F.col(column), "EEEE"))\
                                    .withColumn("monthname", self.date__format(F.col(column), "MMMM")) \
                                    .withColumn("month", self.date__format(F.col(column), "MMMM")) \
                                    .select("occupancydatetime","day_of_week", "monthname","month", PartitionColumn1)\
                                    .drop_duplicates()
                                   
                                   # .select("occupancydatetime","day_of_week", "month", PartitionColumn1)

                date_dim = date_dim.select("occupancydatetime", "day_of_week", "monthname","month",PartitionColumn1)
        
        # drop rows having null values
        select_df=select_df.na.drop(subset=["latitude","longitude"])

        RetryCt = 0
        Success = False

        while (RetryCt < max_retry_count) and not Success:
            try:
                Success = True
              #  select_df.show(3)
                gen_logs.log_print("Writing occupancy dataframe to output file: {}".format(output))

                select_df.write.mode("append").partitionBy(PartitionColumn1,PartitionColumn2).parquet(output)

                gen_logs.log_print("Writing date dimension to output file: {}".format(datedimoutputpath))
              #  date_dim.show(3)
                date_dim.write.mode("append").partitionBy(PartitionColumn1,"month").parquet(datedimoutputpath)
            except:
                Success = False
                RetryCt += 1
                if RetryCt == max_retry_count:
                    gen_logs.log_info(
                        SCRIPT_NAME,
                        "Failed on writing to Output after {} tries: {} ".format(max_retry_count, output),
                    )
                    ReturnCode = 2
                    return ReturnCode, rec_cnt
                else:
                    gen_logs.log_info(
                        SCRIPT_NAME, "Failed on writing to Output, re-try in {} seconds ".format(retry_delay)
                    )
                    time.sleep(retry_delay)

        gen_logs.log_print("Number of Records Processed: {}".format(rec_cnt))
        return ReturnCode, rec_cnt


    def main(self):
        print("Test")

        #    logger = spark._jvm.org.apache.log4j
        #   logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
        #  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

        #self.executeOccupancyOperations(src_df, output, cols_list, cols_dict, partn_col, max_retry_count, retry_delay)


if __name__ == "__main__":
    log_file = "test.log"
    #  gen_logs = GenerateLogs(self.spark)
    #  GenerateLogs.initial_log_file(logfile)
    OccupancyProcessing.main()
