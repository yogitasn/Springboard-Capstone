from dataset_processing.utilities.miscProcess import GenerateLogs

from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, BooleanType, StringType
import re
import time
import sys
import os

SCRIPT_NAME = os.path.basename(__file__)

COMMENT_CHAR = "#"
COMMENT_DATE = datetime.now().strftime("%Y-%m-%d")


class ReadEnvironmentParameters:
    """
    This custom class reads environment variables from a config file.

    """

    def __init__(self, spark):
        self.spark = spark

    def remove_whitespace(self, inParam, accepted_list="[^0-9a-zA-Z/@()_\-. ]+"):
        outParam = re.sub(accepted_list, "", inParam)
        return outParam

    # Parse Parameter files
    def parse_config(self, caller_function, filename, option_char="="):
        gen_logs = GenerateLogs(self.spark)

        ReturnCode = 0
        OPTION_CHAR = option_char
        options = {}
        param_list = caller_function + "\n"

        f = open(filename)

        for line in f:
            # Ignore Empty lines
            if not line.strip():
                continue
            # First, remove comments:
            if COMMENT_CHAR in line:
                # if first char is '#' on the line, skip
                strip_line = line.strip()
                if strip_line[0] == "#":
                    continue
                # split on comment char, keep on the part before
                line, comment = line.split(COMMENT_CHAR, 1)
                line += "\n"

            # Second, find lines with an option = value
            if OPTION_CHAR in line:
                param_list += "{}".format(line)
                # spliy on option char
                option, value = line.split(OPTION_CHAR, 1)
                # strip spaces:

                option = option.strip()
                value = value.strip()

                #value = self.remove_whitespace(value)
                options[option] = value

            else:
                gen_logs.log_error(SCRIPT_NAME, "ERROR: WRONG PARAMETER ASSIGNMENT ON LINE: {}".format(line.strip()), 1)
                ReturnCode = 1
                break

        f.close()
        gen_logs.log_info(SCRIPT_NAME, param_list)
    #    print("Options are {}".format(options))
        return options, ReturnCode

    def read_job_control(self, paramFile):
        param, ReturnCode = self.parse_config("Job Control Parameters", paramFile, "=")
        #globals().update(param)
     #   print(param)
        return param, ReturnCode

    def main(self, paramFile):

        paramFile, ReturnCode = self.read_job_control(paramFile)


if __name__ == "__main__":
    log_file = "test.log"
    GenerateLogs.initial_log_file(log_file)
    ReadEnvironmentParameters.main(sys.argv[1])
