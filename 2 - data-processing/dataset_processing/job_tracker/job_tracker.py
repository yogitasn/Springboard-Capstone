import datetime
import psycopg2
import configparser
import random
import logging
import logging.config
from psycopg2 import sql
from pathlib import Path

from pyspark.sql.functions import last


class JobTracker:

    def __init__(self, psql_host, psql_user_name,psql_pass):
        
        self.psql_host = psql_host
        self.psql_user_name = psql_user_name
        self.psql_pass = psql_pass

    def create_job_table(self):
        """CREATE A JOB STATUS TRACKING TABLE"""
        sql = """CREATE TABLE IF NOT EXISTS spark_job
                (job_id INTEGER NOT NULL,
                job_name VARCHAR(20),
                status VARCHAR(10),
                dataset VARCHAR(30),
                loadtype VARCHAR(30),
                step INTEGER,
                stepdesc VARCHAR(50),
                year_processed INTEGER,
                date date,
                last_date_processed timestamp
                
                );"""

        try:
            conn = self.get_db_connection()
            # create a new cursor
            cur = conn.cursor()
            # execute the CREATE statement
            cur.execute(sql)

            conn.commit()
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    """ Insert the job detail records in the control table """

    def insert_job_details1(self, job_id, job_name, status, dataset, loadtype, step, stepdesc, year_processed, date):
        """insert job details into the table"""
        sql = """insert into spark_job (job_id, job_name, status, dataset, loadtype, step, stepdesc,year_processed, date)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        conn = None
        vendor_id = None
        try:
            conn = self.get_db_connection()
            # create a new cursor
            cur = conn.cursor()
            # execute the INSERT statement
            cur.execute(sql, (job_id, job_name, status, dataset, loadtype, step, stepdesc, year_processed, date))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def insert_job_details(self, job_id, job_name, status, dataset, loadtype, step, stepdesc, year_processed, date,last_date_processed):
      """insert job details into the table"""
      sql = """insert into spark_job (job_id, job_name, status, dataset, loadtype, step, stepdesc,year_processed, date,last_date_processed)
              values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
      conn = None
      vendor_id = None
      try:
          conn = self.get_db_connection()
          # create a new cursor
          cur = conn.cursor()
          # execute the INSERT statement
          cur.execute(sql, (job_id, job_name, status, dataset, loadtype, step, stepdesc, year_processed, date,last_date_processed))
          conn.commit()
          cur.close()
      except (Exception, psycopg2.DatabaseError) as error:
          print(error)
      finally:
          if conn is not None:
              conn.close()

    """ Get the job status of the historic data processing """

    def get_historic_job_status(self, year):
        # connect db and send sql query
        table_name = "spark_job"
        sql = """SELECT spark_job.status FROM spark_job where spark_job.year_processed= %s ;"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            print(" Fetching status of the job from the table")
            cursor.execute(sql, (year,))
            # status = cursor.fetchone()[0]
            status = cursor.fetchall()
            if status == []:
                return "No entry"
            else:
                for s in status:
                   if 'Success' in str(s):
                       return s[0]
                return status[0][0]
               
        except (Exception, psycopg2.Error) as error:
            print("Error getting value from the Database {}".format(error))
            return

    def get_last_processed_date(self,year,dataset):
      # connect db and send sql query
        sql = """SELECT MAX(spark_job.last_date_processed) FROM spark_job WHERE spark_job.year_processed=%s and spark_job.dataset=%s and spark_job.status='Success';"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        try:
            print(" Fetching last date processed for the delta dataset from the table for the year {}".format(year))
            cursor.execute(sql, (year,dataset,))
            # status = cursor.fetchone()[0]
            last_date_processed = cursor.fetchall()
            print(last_date_processed)
            print(last_date_processed[0][0])
            if last_date_processed == [] or last_date_processed[0][0]==None:
                print("last processed date is {}".format(last_date_processed))
                return "No entry"
            else:
                return last_date_processed[0][0]

        except (Exception, psycopg2.Error) as error:
            print("Error getting value from the Database {}".format(error))
            return

    """ Function to connect to POSTGRES database """

    def get_db_connection(self):
        conn = None        

        try:
            # Construct connection string
            conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(self.psql_host, self.psql_user_name, "postgres", self.psql_pass, "require")
            conn = psycopg2.connect(conn_string)
            print("Connection established")

#            print(" Successfully connected to postgres DB")
        except (Exception, psycopg2.Error) as error:
            logging.error("Error while connecting to PostgreSQL {}".format(error))

        return conn
