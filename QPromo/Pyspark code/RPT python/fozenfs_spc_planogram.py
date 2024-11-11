from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt
from cip.cip.dataschema.staging.rangingSchema import schema
import subprocess
import datetime
from cip.cip.framework.files.generic_processes import purge_data,archive_file
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, TimestampType, ShortType, IntegerType, DecimalType
import pyspark.sql.functions as F

class new:

    def __init__(self):
        """
        Assigning Variable from config file
        """
        today = datetime.datetime.today()
        today = today.strftime('%Y%m%d')
        self.target_db =  cnt["Customer_Staging_Mart"]["csm_database"]["landing_database"]
        self.target_table = "cdd_raw_qpromo_spc_planogram"
        self.path = "/user/svc_uk_cust_rdl/sourceFiles/ftdw/QPROMO/QPROMO_ix_spc_planogram.csv"
        self.archive_path = str(cnt["run_path"]["archive_path"]) + '/' + str(today) + str(self.path)
        self.schema = self.new_Schema()

    @staticmethod
    def new_Schema():
        new_Schema = StructType([
			StructField("desc1", StringType(), True),
			StructField("desc2", StringType(), True),
			StructField("dbkey", IntegerType(), True),
			StructField("DBDATEEFFECTIVEFROM", TimestampType(), True),
			StructField("DBDATEEFFECTIVETO", TimestampType(), True),
			StructField("dbstatus", IntegerType(), True),
        ])
        return new_Schema

    def run(self):
        print("************** SPARK JOB Inititated****************")
        source_df = spark.read.format("csv").schema(self.schema).option("header", "false").option("delimiter",'|').option("inferSchema","true").load(self.path)

        source_df.write.mode("overwrite").saveAsTable("{}.{}".format(self.target_db, self.target_table))

        #archive source file once processed
        #archive_file(self.path, self.archive_path)

        print("************** SPARK JOB complete****************")
		
ts = new()
ts.run()
