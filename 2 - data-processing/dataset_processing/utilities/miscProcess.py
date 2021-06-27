import os, sys, subprocess
from datetime import datetime
import pandas as pd
from pandas import Series, DataFrame


class GenerateLogs:
    """
    This custom class generates execution logs in a specific format and saves to a dataframe.
    This is useful in tracking and troubleshooting

    """

    def __init__(self, spark):
        self.spark = spark

    def global_SQLContext(self, spark1):
        global spark
        self.spark = spark1

    # def rename_log_filename(self, logfile):
    #     os.rename(log_filename, logfile)
    #     log_filename = logfile

    def initial_log_file(self, logfile):
        global log_filename, g_main_script
        # Get current system timestamp
        log_filename = logfile

        current_time = datetime.now()
        str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
        time_index = current_time.strftime("%d%H%M%S%f")

        message_list = []

        # Define log file name
        parse = sys.argv[0].split("/")
        g_main_script = parse[-1]

        #self.log_print("Log filename: {}".format(logfile))

        message_list.append(
            "============================================================================================"
        )
        message_list.append("===== SCRIPT: {} started at {} ====".format(g_main_script, str_current_time))
        message_list.append(
            "============================================================================================"
        )
        message_dict = {}
        message_dict = {"Message": message_list, "TimeIndex": time_index}

        message_dict = {}

        message_dict = {"Message": message_list, "TimeIndex": time_index}
        df = pd.DataFrame(message_dict, columns=["Message", "TimeIndex"])
        df = self.spark.createDataFrame(df)
        df.coalesce(1).write.mode("overwrite").format("text").partitionBy("TimeIndex").save(log_filename)
    
    def log_print(self, log_message):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
        time_index = current_time.strftime("%d%H%M%S%f")

        message_list = []

        display_message = str(log_message)
        message_list.append(display_message)
        message_list.append(" ")

        message_dict = {}
        message_dict = {"Message": message_list, "TimeIndex": time_index}

        df = pd.DataFrame(message_dict, columns=["Message", "TimeIndex"])
        df = self.spark.createDataFrame(df)

        df.coalesce(1).write.mode("append").format("text").partitionBy("TimeIndex").save(log_filename)


    def complete_log_file(self):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
        time_index = current_time.strftime("%d%H%M%S%f")

        message_list = []

        message_list.append(
            "============================================================================================"
        )
        message_list.append("===== SCRIPT: {} completed at {} ====".format(g_main_script, str_current_time))
        message_list.append(
            "============================================================================================"
        )
        message_dict = {}
        message_dict = {"Message": message_list, "TimeIndex": time_index}

        message_dict = {}

        message_dict = {"Message": message_list, "TimeIndex": time_index}
        df = pd.DataFrame(message_dict, columns=["Message", "TimeIndex"])
        df = self.spark.createDataFrame(df)

        df.coalesce(1).write.mode("append").format("text").partitionBy("TimeIndex").save(log_filename)

    def log_info(self, caller_script, log_message="Process Completed Successfully"):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
        time_index = current_time.strftime("%d%H%M%S%f")

        message_list = []
        message_list.append("===== SCRIPT: {} completed at {} ====".format(g_main_script, str_current_time))

        display_message = "{}".format(log_message)

        message_list.append(display_message)
        message_list.append(" ")

        message_dict = {}
        message_dict = {"Message": message_list, "TimeIndex": time_index}

        df = pd.DataFrame(message_dict, columns=["Message", "TimeIndex"])
        df = self.spark.createDataFrame(df)

        df.coalesce(1).write.mode("append").format("text").partitionBy("TimeIndex").save(log_filename)

    def log_error(self, caller_script, log_message="Script Completed Successfully", return_code=123):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
        time_index = current_time.strftime("%d%H%M%S%f")

        message_list = []

        if return_code == 123:
            rc_message = ""
        else:
            rc_message = " -RC: " + str(return_code)
            border = "!!!"

        top_message = caller_script + ": " + str_current_time
        str_message = "{} {}".format(log_message, rc_message)
        # log_length = min(len(top_message), len(str_message))
        log_length = len(str_message)

        if log_length > 120:
            log_length = 120

        log_message = "=========="

        for i in range(0, log_length):
            log_message += "="

        border = "!!!"

        message_list.append("{}".format(log_message))
        log_message = border + " " + str_message

        message_list.append("{}".format(log_message))
        #   log_message = border+ ' ' +top_message

        # message_list.append("{}".format(log_message))
        log_message = "=========="

        for i in range(0, log_length):
            log_message += "="

        message_list.append("{}".format(log_message))
        message_list.append(" ")

        message_dict = {}
        message_dict = {"Message": message_list, "TimeIndex": time_index}

        df = pd.DataFrame(message_dict, columns=["Message", "TimeIndex"])
        df = self.spark.createDataFrame(df)

        df.coalesce(1).write.mode("append").format("text").partitionBy("TimeIndex").save(log_filename)

        return return_code

    def log_step(self, caller_script, log_message="Script Completed Successfully"):
        current_time = datetime.now()
        str_current_time = current_time.strftime("%d %b %Y %H:%M:%S.%f")
        time_index = current_time.strftime("%d%H%M%S%f")

        message_list = []

        str_message = caller_script + " " + log_message
        if len(str_message) > 120:
            log_length = 120
        else:
            log_length = len(str_message)

        log_message = "==="

        for i in range(0, log_length):
            log_message += "="

        log_message += "==="

        message_list.append("{}".format(log_message))
        log_message = "** " + str_message + " **"

        message_list.append("{}".format(log_message))
        log_message = "===="

        for i in range(0, log_length):
            log_message += "="

        log_message += "==="
        message_list.append("{}".format(log_message))
        log_message = "Started: {}".format(str_current_time)
        message_list.append("{}".format(log_message))
        message_list.append(" ")
        message_dict = {}

        message_dict = {}
        message_dict = {"Message": message_list, "TimeIndex": time_index}

        df = pd.DataFrame(message_dict, columns=["Message", "TimeIndex"])
        df = self.spark.createDataFrame(df)

        df.coalesce(1).write.mode("append").format("text").partitionBy("TimeIndex").save(log_filename)


if __name__ == "__main__":
    GenerateLogs.log_error(sys.argv[0], sys.argv[1], sys.argv[2])
