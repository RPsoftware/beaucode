%run /3rdparty/quantium/decryptFunction

# COMMAND ----------

import pyspark.sql.functions as F
import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType, DateType, ByteType, IntegerType
from datetime import datetime, date, timedelta
import os
import time

# COMMAND ----------

class ProcessingFiles:
 
    def __init__(self):

        #set env variables
        self.env = os.getenv('env')
        self.loc = 'uks'
        self.instance = '01'
        self.rawMount = '/mnt/raw/'

        self.date = datetime.now() 
        self.dateFolder = self.date.strftime('%Y%m%d')

        ## Key Vault variables
        self.dbScope = 'kv-sa-lnd-{}-{}-{}'.format(self.env,self.loc,self.instance)
        self.secretKey = 'dbr-spn'
        self.clientId = 'spn-AppId'
        self.key = dbutils.secrets.get(scope='kv-sa-lnd-{}-{}-{}'.format(self.env, self.loc, self.instance), key='quantium-cip-decrypt-key')

        self.decryptColumnList = ['unified_cust_id']
        self.mergeFile1 = 'PriceSensitivitySegmentation.parquet'
        self.mergeFile2 = 'ValueSegmentation.parquet'

        self.ErrorMsg = []
 
        self.sourcePath = ('{}/quantium/download/{}/inbound/qpromo/ingested_files/'.format(self.rawMount,self.env))
        self.outputPath = ('{}/quantium/download/{}/inbound/qpromo/processed_files/'.format(self.rawMount,self.env))
        self.archivePath = ('{}quantium/download/{}/inbound/qpromo/archived_files/{}'.format(self.rawMount, self.env, self.dateFolder)) 

    def ReadParquet (self,sourcePath, filename):
        sourceDF = spark.read.parquet(sourcePath + filename)
        return(sourceDF)
    
    def WriteParquet(self,df, outputPath, filename):
        print('writing file to processed_files...')
        try:
            df.write.mode("overwrite").parquet(outputPath + filename)
        except:
            self.ErrorMsg = self.ErrorMsg.append('Failed to write file {}'.format(filename))
        return(True)
    
    def ApplySchema(self,df, filename):
        if filename == 'CustomerActivity.parquet':
            df = df.select(F.col("CUSTOMER_SRC_ID").alias('unified_cust_id'),
                                           F.col('IS_ACTIVE').alias('is_active_cip')
                                           )

        elif filename == 'PriceSensitivitySegmentation.parquet':
            df = df.select(F.col('CUSTOMER_ID').alias('unified_cust_id'),
                                                                    F.col('SEGMENT_ID').alias('seg_value_id'),
                                                                    F.lit('5').alias('segment_id'),
                                                                    F.col('EFF_FROM').alias('eff_from_dt'),
                                                                    F.col('EFF_TO').alias('eff_to_dt')) 

        elif filename == 'ValueSegmentation.parquet':
            df = df.select(F.col('CUSTOMER_ID').alias('unified_cust_id'),
                                    F.col('SEGMENT_ID').alias('seg_value_id'),
                                    F.lit('6').alias('segment_id'), 
                                    F.col('EFF_FROM').alias('eff_from_dt'),
                                    F.col('EFF_TO').alias('eff_to_dt'))

        elif filename == 'qpromo_item_brandoverride.parquet':
            df = df.select(F.col('PRODUCT_NAME'),
                                                 F.col('ARTICAL_NBR').alias('ARTICLE_NBR'), 
                                                 F.col('ORIGINAL_CIN').cast(IntegerType()).alias("ORIGINAL_CIN"),
                                                    F.col('CATEGORY_ID').cast(IntegerType()).alias("CATEGORY_ID"),
                                                    F.col('CATEGORY_NAME'),
                                                    F.col('DEPARTMENT_ID').cast(IntegerType()).alias("DEPARTMENT_ID"),
                                                    F.col('DEPARTMENT_NAME'),
                                                    F.col('MERCHANDISING_CATEGORY_ID').cast(IntegerType()).alias("MERCHANDISING_CATEGORY_ID"),
                                                    F.col('MERCHANDISING_CATEGORY_NAME'),
                                                    F.col('MERCHANDISING_SUBCATEGORY_ID').cast(IntegerType()).alias("MERCHANDISING_SUBCATEGORY_ID"),
                                                    F.col('MERCHANDISING_SUBCATEGORY_NAME'),
                                                    F.col('PRODUCT_PROFILE_GROUP_ID').cast(IntegerType()).alias("PRODUCT_PROFILE_GROUP_ID"),
                                                    F.col('PRODUCT_PROFILE_GROUP_NAME'),
                                                    F.col('SUPER_BRAND'),
                                                    F.col('BRAND_NM'),
                                                    F.col('SUB_BRAND'),
                                                    F.col('PRODUCT_VARIANT'),
                                                    F.col('PRODUCT_SUB_VARIANT'),
                                                    F.col('VENDOR_ID').cast(IntegerType()).alias("VENDOR_ID"),
                                                    F.col('VENDOR_NAME'),
                                                    F.col('GRANDPARENT_VENDOR_ID').cast(IntegerType()).alias("GRANDPARENT_VENDOR_ID"),
                                                    F.col('GRANDPARENT_VENDOR_NAME'),
                                                    F.col('HFSS'),
                                                    F.col('ORGANIC'),
                                                    F.col('FREEFROM'),
                                                    F.col('VEGAN'),
                                                    F.col('VEGETARIAN'),
                                                    F.col('WORLD_FOOD'),
                                                    F.col('LOCAL'),
                                                    F.col('COUNTRY_OF_ORIGIN'),
                                                    F.col('FLAVOUR'),
                                                    F.col('COLOUR'),
                                                    F.col('SINGLE_SIZE').cast(IntegerType()).alias("SINGLE_SIZE"),
                                                    F.col('SINGLE_SIZE_UOM'),
                                                    F.col('SINGLE_SIZE_WITH_UNITS'),
                                                    F.col('PACK_SIZE'),
                                                    F.col('PACK_SIZE_UOM'),
                                                    F.col('PACK_SIZE_WITH_UNITS'),
                                                    F.col('TOTAL_SIZE').cast(IntegerType()).alias("TOTAL_SIZE"),
                                                    F.col('TOTAL_SIZE_UOM'),
                                                    F.col('TOTAL_SIZE_WITH_UNITS'),
                                                    F.col('ALCOHOL_BAND'),
                                                    F.col('ALCOHOL_VOLUME'),
                                                    F.col('BASE_TYPE'),
                                                    F.col('DIET'),
                                                    F.col('PACK_CONFIGURATION'),
                                                    F.col('PACK_TYPE'),
                                                    F.col('PRICE_BANDS'),
                                                    F.col('REGION'),
                                                    F.col('CDT_1'),
                                                    F.col('CDT_2'),
                                                    F.col('CDT_3'),
                                                    F.col('CDT_4'),
                                                    F.col('CDT_5'),
                                                    F.col('CDT_6'),
                                                    F.col('CDT_7'),
                                                    F.col('CDT_8'),
                                                    F.col('CDT_9'),
                                                    F.col('CDT_10'),
                                                    F.col('CDT_11'),
                                                    F.col('CDT_12'),
                                                    F.col('CDT_13'),
                                                    F.col('CDT_14'),
                                                    F.col('CDT_15'))
        
        elif filename == 'qpromotions.parquet':
            df = df
        
        elif filename == 'events.parquet':
            df.createOrReplaceTempView('events')
        
            #create final df for events, replace any NULL date_end with date_start as this represents a single day event
            df = spark.sql("""select DATE_START, CASE WHEN DATE_END IS NULL THEN DATE_START ELSE DATE_END END AS DATE_END, HOLIDAY_NAME, APPLICABLE_TO, cast(INCLUDE_FLAG as int) as include_flag FROM events """)
            print()
        return(df)

    def Decryption(self,df, key, decryptColumnList):
        for x in decryptColumnList:
                df = df.withColumn(x, decrypt(str(x), F.lit(key)))
        
        return(df)
    
    def MergeFiles(self, df1, df2):
        mergedf = df1.union(df2)
        return(mergedf)

  
    def run(self):
        print("************** SPARK JOB Inititated****************")
        #get list of available files to process 
        todayDate = date.today()
        weekday = todayDate.weekday()
        print('weekday = {}'.format(weekday))

        SunFiles = ['CustomerActivity.parquet','PriceSensitivitySegmentation.parquet', 'ValueSegmentation.parquet', 'qpromo_item_brandoverride.parquet', 'events.parquet']

        WedsFiles = ['qpromotions.parquet', 'events.parquet']


        if weekday == 0:
            availableFiles = SunFiles
        elif weekday == 2:
            availableFiles = WedsFiles
        elif weekday != 0 or weekday != 2: 
            availableFiles = ['events.parquet']

        print(availableFiles)
        
        #for each available file, process according to requirements
        for f in availableFiles:
            try: 
                print('\n' + f)
                try:
                    filecheck = dbutils.fs.ls(self.sourcePath + f)
                    print('{} found at sourcePath'.format(f))
                except:
                    self.ErrorMsg.append('Failed to find file at source')
                    print('creating .parquet file in processed_files...')
                try:
                    rawdf = self.ReadParquet(self.sourcePath, f)
                except: 
                    self.ErrorMsg.append('Failed to read source file {}'.format(f))
                try:
                    schemadf = self.ApplySchema(rawdf, f)
                    try:
                        for c in schemadf.columns:
                            if c in self.decryptColumnList:
                                schemadf = self.Decryption(schemadf, self.key, self.decryptColumnList)
                            else: pass
                    except:
                        self.ErrorMsg.append('Failed to decrpyt columns')
                    try:
                        schemadf = schemadf.distinct() 
                        schemadf.printSchema()
                    except:
                        self.ErrorMsg.append('Failed to get distinct rows')
                except:
                    self.ErrorMsg.append(str('Failed to apply schema correctly for {}'.format(f)))
                try:
                    self.WriteParquet(schemadf, self.outputPath, f)
                except:
                    self.ErrorMsg.append('Failed to write file {}'.format(f))
                        #merge segments
                try:
                    if weekday == 0:
                        df1 = spark.read.parquet(self.outputPath + self.mergeFile1)
                        df2 = spark.read.parquet(self.outputPath + self.mergeFile2)
                        mergedf = self.MergeFiles(df1, df2).distinct()
                        self.WriteParquet(mergedf, self.outputPath, 'CIPSegments.parquet') 
                except:
                    print(Exception)
                    self.ErrorMsg.append('Unable to create CIPSegments')

            except:
                self.ErrorMsg.append('An unknown error occured')
        
        print("************** SPARK JOB complete****************")
        print(self.ErrorMsg)
        if self.ErrorMsg != None:
            dbutils.notebook.exit(self.ErrorMsg)
            
aa = ProcessingFiles()
aa.run()
