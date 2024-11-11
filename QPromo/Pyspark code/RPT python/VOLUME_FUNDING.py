from cip.cip.framework.connections.spark import spark
import datetime
import subprocess
import pyspark.sql.functions as F
from cip.cip.framework.files.generic_processes import purge_data,archive_file

class new_table:
    def __init__(self):
        """
        Assigning Variable from config file
        """
               
        self.raw_db = 'gb_customer_data_domain_raw'

        self.tmp = 'cdd_raw_tmp_AMS_MICOE_Volume_Agreement_GB'
        self.og = 'cdd_raw_AMS_MICOE_Volume_Agreement_GB'
        
        self.target_table = 'cdd_raw_bck_AMS_MICOE_Volume_Agreement_GB'

    def run(self):
        print("************** SPARK JOB Inititated****************")

        og_df = spark.sql(""" select * from {}.{} where ldm_dt_lastload < '2020-08-26' """.format(self.raw_db, self.og))
        tmp_df = spark.sql(""" select * from {}.{} """.format(self.raw_db, self.tmp))
        
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        source_df = og_df.union(tmp_df)

        #initial load
        source_df.write.mode("overwrite").saveAsTable("{}.{}".format(self.raw_db, self.target_table))

        print("*****************END*********************************")

ts = new_table()
ts.run()