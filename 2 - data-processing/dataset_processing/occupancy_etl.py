#!/anaconda3/envs/dbconnect/python.exe
#import findspark

#findspark.init()

#findspark.find()

import pyspark

#findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, DateType, TimestampType, DecimalType
import pyspark
from pyspark.sql import SparkSession
from pathlib import Path
import logging
import logging.config
import configparser
import time
from datetime import datetime
from pathlib import Path

from dataset_processing.utilities.miscProcess import GenerateLogs
from dataset_processing.utilities.processDataframeConfig import DataframeConfig
from dataset_processing.blockface_processing.executeBlockface import BlockfaceProcessing
from dataset_processing.utilities.readEnvironmentParameters import ReadEnvironmentParameters
from dataset_processing.occupancy_processing.executeOccupancyProcess import OccupancyProcessing
from dataset_processing.job_tracker.job_tracker import JobTracker


import sys
import os
import glob
import uuid

path = str(Path(Path(__file__).parent.absolute()).parent.absolute())
logging.info(path)

print(path)

SCRIPT_NAME = os.path.basename(__file__)

try:
    sys.path.insert(1, path+"dataset_processing/data") # the type of path is string
    sys.path.insert(2, path+"dataset_processing/")

except (ModuleNotFoundError, ImportError) as e:
    print("{} fileure".format(type(e)))

# create spark session
def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.\
            builder.\
            appName("Seattle Parking Occupancy ETL").\
            config("spark.sql.sources.commitProtocolClass","org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol").\
            config("parquet.enable.summary-metadata","false").\
            config("mapreduce.fileoutputcommitter.marksuccessfuljobs","false").\
            getOrCreate()

# get dbutils object

spark = create_sparksession()

dbutils = None

#if spark.conf.get("spark.databricks.service.client.enabled") == "true":
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)
print(dbutils.fs.ls("dbfs:/"))
print(dbutils.secrets.listScopes())

def get_dir_content(ls_path):
  dir_paths = dbutils.fs.ls(ls_path)
  subdir_paths = [get_dir_content(p.path) for p in dir_paths if p.isDir() and p.path != ls_path]
  flat_subdir_paths = [p for subdir in subdir_paths for p in subdir]
  return list(map(lambda p: p.path, dir_paths)) + flat_subdir_paths

     
def main(jobname, logfilename, spark_submit_mode):
    """
    Args:
        jobname: Job name
        logfilename: File name of the log file
        spark_submit_mode: Char value Y or N
    """
    """
    Driver code to perform the following steps
    1. Mounts the raw historical and delta Seattle Paid Parking Occupancy and Blockface files to DBFS
    2. Code to handle one- time execution of historical data from 2012-2020 using Lambda architecture concept
        - Execution status stored in postgres table and used to check the status before processing
    3. Code to process the previous processed last date in delta loads and processes the data after the last date
    4. Pyspark code to perform transformations on the historic and delta datasets and save to final parquet files.

    """
    logging.debug("\n\nSetting up Spark Session...")
    # =======================================================================================================#
    # INITIALIZATION
    # =======================================================================================================#
    """
    Pass command-line arguments to execute the ETL driver python file.

    """

    JOBNAME = jobname
    print("JOBNAME is {}".format(JOBNAME))
    LogFileName = logfilename

    ControlPath = "./"
    SparkSubmitClientMode = spark_submit_mode

    PROCESS_TYPE = "POC"

    BlockfaceDataframeName = "blockface"
    OccupancyDataframeName = "occupancy"


    job_id = str(uuid.uuid4().fields[-1])[:5]

    print(" Job ID created {}".format(job_id))


    # =========================================================================================================
    # ================ Open Spark Context Session =============================================================
    # =========================================================================================================

    spark = create_sparksession()

    # Make the SQLContext Session available to sub-scripts
    blockface_pr = BlockfaceProcessing(spark)

    occupancy_pr = OccupancyProcessing(spark)
    readEnvParameters = ReadEnvironmentParameters(spark)

    dataframeconfig = DataframeConfig(spark)

    gen_logs = GenerateLogs(spark)


    # ==========================================================================================================================#
    # # Python code to mount and access Azure Data Lake Storage Gen2 Account to Azure Databricks with Service Principal and OAuth
    # ==========================================================================================================================#

    # Define the variables used for creating connection strings
    #dbutils = get_dbutils(spark)

    # Define the variables used for creating connection strings
    adlsAccountName = "seattledatalake16"
    #mount historical
    adlsContainerName = "raw"
    adlsHistFolderName = "historical"
    mountHistPoint = "/mnt/raw/historical"

    adlsLogsContainerName = "logs"
    adlsLogsFolderName = "execution_logs"
    mountLogsPoint = "/mnt/logs/execution_logs"

    # mount delta
    adlsDeltaFolderName = "delta"
    mountDeltaPoint = "/mnt/raw/delta"

    # mount blockface
    adlsBlocFolderName = "blockface"
    mountBlocPoint = "/mnt/raw/blockface"

    # output folder
    adlsOutContainerName = "processed"
    adlsParkOutFolderName = "ParkingOutput"
    mountParkOutPoint = "/mnt/processed/ParkingOutput"

    adlsDateDimOutFolderName = "DateDimOutput"
    mountDateDimOutPoint = "/mnt/processed/DateDimOutput"

    adlsBlockOutFolderName = "BlockfaceOutput"
    mountBlockOutPoint = "/mnt/processed/BlockfaceOutput"

    # Application (Client) ID
    applicationId = dbutils.secrets.get(scope="datalake16",key="client_id")

    # Application (Client) Secret Key
    authenticationKey = dbutils.secrets.get(scope="datalake16",key="client_secret")

    # Directory (Tenant) ID
    tenandId = dbutils.secrets.get(scope="datalake16",key="tenant_id")

    endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
    source_logs = "abfss://" + adlsLogsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsLogsFolderName
    source_hist = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsHistFolderName
    source_delta = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsDeltaFolderName
    source_bloc = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsBlocFolderName
    source_park_out = "abfss://" + adlsOutContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsParkOutFolderName
    source_datedim_out = "abfss://" + adlsOutContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsDateDimOutFolderName
    source_block_out = "abfss://" + adlsOutContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsBlockOutFolderName


    # Connecting using Service Principal secrets and OAuth
    configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": applicationId,
            "fs.azure.account.oauth2.client.secret": authenticationKey,
            "fs.azure.account.oauth2.client.endpoint": endpoint}

    # Mounting ADLS Storage to DBFS
    # Mount only if the directory is not already mounted
    if not any(mount.mountPoint == mountLogsPoint for mount in dbutils.fs.mounts()):
        print("Mounting Logs")
        dbutils.fs.mount(
            source = source_logs,
            mount_point = mountLogsPoint,
            extra_configs = configs)

    if not any(mount.mountPoint == mountHistPoint for mount in dbutils.fs.mounts()):
        print("Mounting Historical")
        dbutils.fs.mount(
            source = source_hist,
            mount_point = mountHistPoint,
            extra_configs = configs)

    if not any(mount.mountPoint == mountDeltaPoint for mount in dbutils.fs.mounts()):
        print("Mounting Delta")
        dbutils.fs.mount(
            source = source_delta,
            mount_point = mountDeltaPoint,
            extra_configs = configs)

    if not any(mount.mountPoint == mountBlocPoint for mount in dbutils.fs.mounts()):
        print("Mounting Blockface")
        dbutils.fs.mount(
            source = source_bloc,
            mount_point = mountBlocPoint,
            extra_configs = configs)

    if not any(mount.mountPoint == mountParkOutPoint for mount in dbutils.fs.mounts()):
        print("Mounting Parking Output")
        dbutils.fs.mount(
            source = source_park_out,
            mount_point = mountParkOutPoint,
            extra_configs = configs)

    if not any(mount.mountPoint == mountDateDimOutPoint for mount in dbutils.fs.mounts()):
        print("Mounting Date Dim Output")
        dbutils.fs.mount(
            source = source_datedim_out,
            mount_point = mountDateDimOutPoint,
            extra_configs = configs)

    if not any(mount.mountPoint == mountBlockOutPoint for mount in dbutils.fs.mounts()):
        print("Mounting Blockface Output")
        dbutils.fs.mount(
            source = source_block_out,
            mount_point = mountBlockOutPoint,
            extra_configs = configs)

    
    print("Connecting to Postgres Database")
    psql_host = dbutils.secrets.get(scope = "postgres", key = "psql_host")
    psql_username= dbutils.secrets.get(scope = "postgres", key = "admin_username")
    psql_password= dbutils.secrets.get(scope = "postgres", key = "admin_password")


    job_tracker = JobTracker(psql_host, psql_username,psql_password)

    job_tracker.create_job_table()
    # =======================================================================================================#
    ############################ FUNCTIONS ###################################################################
    # =======================================================================================================#
    """ Track the etl processing status in POSTGRES table """
    def update_control_table(job_id, JOBNAME, status, dataset, loadtype, step, stepdesc, year_processed, date, last_date_processed):
        job_tracker.insert_job_details(job_id, JOBNAME, status, dataset, loadtype, step, stepdesc, year_processed, date, last_date_processed)

    """ Track the etl processing status in POSTGRES table """
    def get_last_date_processed(delta_year):
        get_last_date_processed=job_tracker.get_last_processed_date(delta_year,'Occupancy Dataset')
        return get_last_date_processed

    print("Read Log filename")
    # =========================================================================================================
    # ================ Initialize log Filename ================================================================
    # =========================================================================================================

    gen_logs.initial_log_file("dbfs:{}/{}".format(mountLogsPoint,LogFileName))
    
    print("Read Job Specific parameters")
    # =========================================================================================================
    ###################################PROCESS ALL PARAMETERS##################################################
    STEP, STEP_DESC = (10, "Read Job Specific Parameter Files")
    # =========================================================================================================

    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

    if SparkSubmitClientMode == "Y":
        # Spark Submitted in Client Mode
        job_control_file = "{}/{}/{}.ini".format(path, ControlPath,JOBNAME)
        blockface_config_filename = (
            "{}/{}/{}.json".format(path, ControlPath,BlockfaceDataframeName.lower())
        )
        occupancy_config_filename = (
            "{}/{}/{}.json".format(path,ControlPath,OccupancyDataframeName.lower())
            )     
    else:
        # Spark Submitted in Cluster Mode
        job_control_file = "{}/dataset_processing/{}.ini".format(path,JOBNAME)
        print(job_control_file)
        blockface_config_filename = (
            "{}/dataset_processing/data/{}.json".format(path, BlockfaceDataframeName.lower())
        )
        print(blockface_config_filename)
        occupancy_config_filename = (
            "{}/dataset_processing/data/{}.json".format(path, OccupancyDataframeName.lower())
            )     
        print(blockface_config_filename)


    if os.path.exists(job_control_file):
        gen_logs.log_info(SCRIPT_NAME, "Job control filename: {} exist".format(job_control_file))
        paramFile, ReturnCode = readEnvParameters.read_job_control(job_control_file)
        print(paramFile)

        if ReturnCode != 0:
            gen_logs.log_error(SCRIPT_NAME, "Error : Reading Job Control file {} ".format(job_control_file), ReturnCode)
            dbutils.notebook.exit()
        globals().update(paramFile)
    else:
        gen_logs.log_error(SCRIPT_NAME, "Job control filename: {} doesn't exist ".format(job_control_file), STEP)
        dbutils.notebook.exit()

    print("Validate All Needed Parameters defined from the control files")
    # ==============================================================================================================#
    (STEP, STEP_DESC) = (20, "Validate All Needed Parameters defined from the control files")
    # ===============================================================================================================#
    # ALWAYS PERFORM THIS STEP
    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP: {}: {}".format(STEP, STEP_DESC))


    if "RerunId" not in globals():
        gen_logs.log_error(
            SCRIPT_NAME, "ERROR: Parameter RerunId is not defined on control file: {}".format(JOBNAME + ".ini"), STEP
        )
        dbutils.notebook.exit()

    if "ErrorRetryCount" not in globals():
        ErrorRetryCount = 1
    else:
        ErrorRetryCount = int(globals().get('ErrorRetryCount'))

    if "RetryDelay" not in globals():
        RetryDelay = 600
    else:
        RetryDelay = int(globals().get('RetryDelay'))

    if isinstance("historic_years", list) == False:
        historic_years = ["2012", "2013","2014","2015","2016","2017"]

    print(historic_years)
    if isinstance("recent_years", list) == False:
        recent_years = ["2018","2019","2020"]


    if "StartStep" not in globals():
        StartStep = 5
    else:
        StartStep = int(globals().get('StartStep'))

    if "StopStep" not in globals():
        StopStep = 100
    else:
        StopStep = int(globals().get('StopStep'))

    
    if "max_retry_count" not in globals():
        max_retry_count = 2
    else:
        max_retry_count = int(globals().get('max_retry_count'))

    if "retry_delay" not in globals():
        retry_delay = 100
    else:
        retry_delay = int(globals().get('retry_delay'))
    

    print("Read previous execution runtime control")
    # ==============================================================================================================#
    (STEP, STEP_DESC) = (30, "Read Possible Previous Execution Runtime Control")
    # ==============================================================================================================#
    # ALWAYS PERFORM THIS STEP

    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))

    # Flag to track historic occupancy execution from 2012-2017
    isHistoric = True

    hist_yrs_to_be_proc=[]

    for y in historic_years:
        status = job_tracker.get_historic_job_status(y)
        print("{} for year:{}".format(status, y))
        if status == "Failed" or status == "No entry":
            gen_logs.log_info(SCRIPT_NAME, "Historical data for the year {}  needs to be re processed ".format(y))
            hist_yrs_to_be_proc.append(int(y))
            isHistoric = False

    print("Historic years to be processed {}".format(hist_yrs_to_be_proc))

    # Flag to track historic occupancy execution from 2018 to currentyear-1
    isHistoric1 = True
    recent_years_to_be_proc=[]
    for y in recent_years:
        status = job_tracker.get_historic_job_status(y)
        print("{} for year:{}".format(status, y))
        if status == "Failed" or status == "No entry":
            gen_logs.log_info(SCRIPT_NAME, "Historical data for the year {}  needs to be re processed ".format(y))
            recent_years_to_be_proc.append(int(y))
            isHistoric1 = False

    print("Recent years to be processed {}".format(recent_years_to_be_proc))

    print("Processing Blockface Dataframe configuration file")
    # ==============================================================================================================#
    (STEP, STEP_DESC) = (40, "Processing Blockface Dataframe configuration file")
    # ===============================================================================================================#
    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))
    today = datetime.now()
    current_year = today.year


    if StartStep <= STEP and StopStep >= STEP:
        gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}: {} ".format(STEP, STEP_DESC))
        if os.path.exists(blockface_config_filename):
            gen_logs.log_info(
                SCRIPT_NAME, "Blockface Dataframe Configuration filename: {} exists ".format(blockface_config_filename)
            )
            blockface_config_dict = dataframeconfig.json_reader(blockface_config_filename)
        else:
            gen_logs.log_error(
                SCRIPT_NAME,
                "ERROR: Dataframe Configuration file: {} does not exist ".format(blockface_config_filename),
                STEP,
            )
            dbutils.notebook.exit()


    # Get Dataframe Column List
    cols_list = dataframeconfig.build_dataframe_column_list(blockface_config_dict)

    # Get Blockface file path
    blockfacefilePath = "dbfs:{}".format(mountBlocPoint)

    # Get Target Dataframe Schema
    TargetDataframeSchema = dataframeconfig.get_dataframe_schema(blockface_config_dict)

    # Get Output file path to save processed data
    OutputPath = "dbfs:{}".format(mountBlockOutPoint)

    update_control_table(
        job_id=job_id,
        JOBNAME=JOBNAME,
        status="In Progess",
        dataset="Blockface Dataset",
        loadtype="STATIC",
        step=STEP,
        stepdesc="CompletedStep",
        year_processed=current_year,
        date=datetime.today(),
        last_date_processed= datetime.now(),
    )

    print("Create Dataframe, Build and Execute Blockface Transformation Process")
    # =================================================================
    (STEP, STEP_DESC) = (50, "Create Dataframe, Build and Execute Blockface Transformation Process")
    # ==================================================================
    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))


    if StartStep <= STEP and StopStep >= STEP:
        current_time = datetime.now()
        LoadStartTs = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        gen_logs.log_print("LoadStartTs: {}".format(LoadStartTs))

        # =================================================================
        # == Create Blockface Dataframe from the sources
        # ==================================================================

        (src_df, source_data_info_array) = (None, None)
        try:
            (src_df, source_data_info_array) = blockface_pr.sourceBlockfaceReadParquet(
                blockfacefilePath, TargetDataframeSchema
            )

            print("Blocface dataframe read")

        except Exception as e:
            gen_logs.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
            dbutils.notebook.exit()

        # =================================================================
        # == Create Blockface Transformations on Dataframe
        # ==================================================================
        (ReturnCode, rec_cnt) = blockface_pr.executeBlockfaceOperations(
            src_df, OutputPath, cols_list, max_retry_count, retry_delay
        )

        if ReturnCode != 0:
            gen_logs.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
            update_control_table(
                job_id=job_id,
                JOBNAME=JOBNAME,
                status="Failed",
                dataset="Blockface Dataset",
                loadtype="STATIC",
                step=STEP,
                stepdesc="FailedStep",
                year_processed="2021",
                date=datetime.today(),
                last_date_processed=datetime.now(),
            )
            dbutils.notebook.exit()

        update_control_table(
            job_id=job_id,
            JOBNAME=JOBNAME,
            status="Success",
            dataset="Blockface Dataset",
            loadtype="STATIC",
            step=STEP,
            stepdesc="CompletedStep",
            year_processed="2021",
            date=datetime.today(),
            last_date_processed=datetime.now(),
        )


    print("Processing Occupancy Dataframe configuration file")
    # ==============================================================================================================#
    (STEP, STEP_DESC) = (60, "Processing Occupancy Dataframe configuration file")
    # ===============================================================================================================#


    if StartStep <= STEP and StopStep >= STEP:
        gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}: {} ".format(STEP, STEP_DESC))

        if os.path.exists(occupancy_config_filename):
            gen_logs.log_info(SCRIPT_NAME, "Occupancy Configuration filename: {} exists ".format(occupancy_config_filename))
            occupancy_config_dict = dataframeconfig.json_reader(occupancy_config_filename)
        else:
            gen_logs.log_error(
                SCRIPT_NAME,
                "ERROR: Occupancy Configuration file: {} does not exist ".format(occupancy_config_filename),
                STEP,
            )
            dbutils.notebook.exit()

        # Get Dataframe Column List
        OccpnColumnList = dataframeconfig.build_dataframe_column_list(occupancy_config_dict)

        # Get Column Partition
        PartitionColumn1, PartitionColumn2 = dataframeconfig.partition_columns(occupancy_config_dict)

        # Get Target Dataframe Schema
        TargetOccpDFSchema = dataframeconfig.get_dataframe_schema(occupancy_config_dict)

        # Get Target Historic Dataframe Schema
        TargetHistOccpDFSchema = dataframeconfig.get_historic_dataframe_schema(occupancy_config_dict)

        # Get Occupancy dataset File path
        occupancyHistFilePath = "dbfs:{}".format(mountHistPoint)
        occupancyDeltaFilePath = "dbfs:{}".format(mountDeltaPoint)

        # Get the Occupany processed output path
        OutputPath = "dbfs:{}".format(mountParkOutPoint)

        # Get the Dimension processed output path
        datedimOutputPath = "dbfs:{}".format(mountDateDimOutPoint)

    print("Create Dataframe, Build and Execute Occupancy Process")
    # ===============================================================================
    (STEP, STEP_DESC) = (70, "Create Dataframe, Build and Execute Occupancy Process")
    # ===============================================================================
    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))


    if StartStep <= STEP and StopStep >= STEP:
        current_time = datetime.now()
        LoadStartTs = current_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        gen_logs.log_print("LoadStartTs: {}".format(LoadStartTs))

     #   file_names = glob.glob(occupancyHistFilePath)
        paths = get_dir_content(occupancyHistFilePath)
     
        for file in paths:
            year = file.split("/")[4][:4]
            print(year)
            print(isHistoric)
            print(isHistoric1)

            if int(year) <= 2017 and isHistoric == False and int(year) in hist_yrs_to_be_proc:
                (src_df, source_data_info_array) = (None, None)
                print("Inside historical dataprocess for years less and equal to 2017")
                occupancyFilePath = file

                # =================================================================
                # == Create Occupancy Dataframe from the sources
                # ==================================================================

                try:
                    (src_df, source_data_info_array) = occupancy_pr.sourceOccupancyReadParquet(
                        occupancyFilePath, TargetHistOccpDFSchema, PartitionColumn1,PartitionColumn2
                    )

                except Exception as e:
                    gen_logs.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
                    break

                (ReturnCode, rec_cnt) = occupancy_pr.executeHistoricOccupancyOperations(
                    src_df, OutputPath, datedimOutputPath, OccpnColumnList, PartitionColumn1, \
                        PartitionColumn2, max_retry_count, retry_delay
                )


                if ReturnCode != 0:
                    gen_logs.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
                    update_control_table(
                        job_id=job_id,
                        JOBNAME=JOBNAME,
                        status="Failed",
                        dataset="Occupancy Dataset",
                        loadtype="HISTORIC",
                        step=STEP,
                        stepdesc="FailedStep",
                        year_processed=year,
                        date=datetime.today(),
                        last_date_processed=datetime.now(),
                    )
                    break

                update_control_table(
                    job_id=job_id,
                    JOBNAME=JOBNAME,
                    status="Success",
                    dataset="Occupancy Dataset",
                    loadtype="HISTORIC",
                    step=STEP,
                    stepdesc="CompletedStep",
                    year_processed=year,
                    date=datetime.today(),
                    last_date_processed=datetime.now(),
                )

            elif int(year) >= 2018 and int(year) <= current_year - 1 and isHistoric1 == False and int(year) in recent_years_to_be_proc:
                print("Inside year {}".format(year))
                occupancyFilePath = file

                # =================================================================
                # == Create Occupancy Dataframe from the sources
                # ==================================================================

                (src_df, source_data_info_array) = (None, None)

                try:
                    (src_df, source_data_info_array) = occupancy_pr.sourceOccupancyReadParquet(
                        occupancyFilePath, TargetOccpDFSchema, PartitionColumn1, PartitionColumn2
                    )

                except Exception as e:
                    gen_logs.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
                    dbutils.notebook.exit()

                
                # =================================================================
                # == Create Occupancy Historical Transformations on Dataframe
                # ==================================================================

                (ReturnCode, rec_cnt) = occupancy_pr.executeOccupancyOperations(
                    src_df, OutputPath, datedimOutputPath, OccpnColumnList, PartitionColumn1,PartitionColumn2, max_retry_count, retry_delay
                )

                if ReturnCode != 0:
                    gen_logs.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
                    update_control_table(
                        job_id=job_id,
                        JOBNAME=JOBNAME,
                        status="Failed",
                        dataset="Occupancy Dataset",
                        loadtype="HISTORIC",
                        step=STEP,
                        stepdesc="FailedStep",
                        year_processed=year,
                        date=datetime.today(),
                        last_date_processed=datetime.now(),
                    )
                    break

                update_control_table(
                    job_id=job_id,
                    JOBNAME=JOBNAME,
                    status="Success",
                    dataset="Occupancy Dataset",
                    loadtype="HISTORIC",
                    step=STEP,
                    stepdesc="CompletedStep",
                    year_processed=year,
                    date=datetime.today(),
                    last_date_processed=datetime.now(),
                )

    print("Create Dataframe, Build and Execute Occupancy Delta Process")
    # ====================================================================================
    (STEP, STEP_DESC) = (80, "Create Dataframe, Build and Execute Delta Occupancy Process")
    # =====================================================================================
    gen_logs.log_step(SCRIPT_NAME, "PERFORMING STEP {}:{} ".format(STEP, STEP_DESC))        
        
    if StartStep <= STEP and StopStep >= STEP:
        if current_year:
            print("Inside the delta load process for the year: {}".format(current_year))
            DeltaFilePath = occupancyDeltaFilePath
            # =================================================================
            # == Create Occupancy Dataframe from the sources
            # ==================================================================

            last_date_processed = get_last_date_processed(str(current_year))

            print("Last Date Processed {}".format(last_date_processed))
            
            (src_df, source_data_info_array) = (None, None)

            try:
                (src_df, source_data_info_array) = occupancy_pr.sourceOccupancyReadParquet(
                    DeltaFilePath, TargetOccpDFSchema, PartitionColumn1,PartitionColumn2
                )

            except Exception as e:
                gen_logs.log_error(SCRIPT_NAME, "Source Error: {}".format(e), STEP)
                dbutils.notebook.exit()

            # =================================================================
            # == Create Occupancy Delta Transformations on Dataframe
            # ==================================================================

            (ReturnCode, rec_cnt, max_date, is_processed) = occupancy_pr.executeDeltaOccupancyOperations(
                src_df, OutputPath, datedimOutputPath, last_date_processed, OccpnColumnList, PartitionColumn1,PartitionColumn2, max_retry_count, retry_delay
            )

            print(max_date)

            if ReturnCode != 0:
                gen_logs.log_error(SCRIPT_NAME, "Error Processing Transformation Failed ", STEP)
                update_control_table(
                    job_id=job_id,
                    JOBNAME=JOBNAME,
                    status="Failed",
                    dataset="Occupancy Dataset",
                    loadtype="DELTA",
                    step=STEP,
                    stepdesc="FailedStep",
                    year_processed=current_year,
                    date=datetime.today(),
                    last_date_processed=max_date,
                )
                dbutils.notebook.exit()
                
            print("Writing Delta Status to Postgres table")
            if is_processed ==False:
                update_control_table(
                    job_id=job_id,
                    JOBNAME=JOBNAME,
                    status="Success",
                    dataset="Occupancy Dataset",
                    loadtype="DELTA",
                    step=STEP,
                    stepdesc="CompletedStep",
                    year_processed=current_year,
                    date=datetime.today(),
                    last_date_processed=max_date,
                )

        gen_logs.complete_log_file()



# Entry point for the pipeline
if __name__ == "__main__":
    #main()
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("caller_jobname", type=str)
    parser.add_argument("log_filename", type=str)
    parser.add_argument("spark_client_mode", type=str)

    # Parse the cmd line args
    args = parser.parse_args()
    #args = vars(args)

    caller_jobname= args.caller_jobname
    log_filename=args.log_filename
    spark_client_mode=args.spark_client_mode

    main(caller_jobname,log_filename,spark_client_mode)
