from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt
import pyspark.sql.functions as F

# import static references for hdfs from 'py4j.java_gateway.JavaClass'> (conf, filesystem, path, fileutil)

sc = spark.sparkContext
fileutil = sc._jvm.org.apache.hadoop.fs.FileUtil
hdfs_path = sc._jvm.org.apache.hadoop.fs.Path

filesystem = sc._jvm.org.apache.hadoop.fs.FileSystem
jvm_conf = sc._jsc.hadoopConfiguration()
hdfs_fs = filesystem.get(jvm_conf)

class CDP:

    def __init__(self):
        """
        Assigning Variable from config file
        """
        self.source_db = cnt["Customer_Staging_Mart"]["cdd_rpt_database"]["extraction_database"]
        self.source_table = cnt["Customer_Staging_Mart"]["cdd_rpt_tables"]["cdd_rpt_cip_product"]
        self.file_path = str(cnt["run_path"]["root_path"]) + str(cnt["file_path"]["CDP"]["output"])

    def file_run(self):
        print("*****************Extraction Start******************")


        extract_df = spark.sql("SELECT cons_item_nbr as consumerItemNum, latest_item_nbr as WIN, product_desc as name, brand_nm as brandName, coalesce(catg_nbr,'') as categoryId, \
				coalesce(catg_desc,'') as categoryName,dept_nbr as departmentId,dept_desc as departmentName,vendor_nbr as supplierId,vendor_nm as supplierDetails,coalesce(baby_new_born,'') as babyNewBorn, \
				coalesce(toddler,'') as toddler,coalesce(adventurous,'') as adventurous,coalesce(children,'') as children,coalesce(convenience_readymeals,'') as convenienceReadymeals, \
				coalesce(convenience_timesaving,'') as convenienceTimesaving,coalesce(ethical,'') as ethical,coalesce(healthy,'') as healthy,coalesce(high_calorie,'') as highCalorie, \
				coalesce(low_calorie,'') as lowCalorie,coalesce(scratch_cooking,'') as scratchCooking,coalesce(traditional,'') as traditional,coalesce(vegetarian,'') as vegetarian, \
				coalesce(own_label,'') as ownLabel,coalesce(branded,'') as branded,coalesce(own_label_choice,'') as ownLabelChoice,coalesce(branded_choice,'') as brandedChoice, \
				coalesce(own_label_smartprice,'') as ownLabelSmartprice,coalesce(own_label_extraspecial,'') as ownLabelExtraspecial,coalesce(own_label_goodforyou,'') as ownLabelGoodforyou, \
				coalesce(own_label_freefrom,'') as ownLabelFreefrom,coalesce(own_label_standard,'') as ownLabelStandard,coalesce(own_label_asdaother,'') as ownLabelAsdaother, \
				coalesce(organic,'') as organic,coalesce(essentials_staples,'') as essentialsStaples,coalesce(essentials_core,'') as essentialsCore,coalesce(essentials_occasional,'') as essentialsOccasional, \
				coalesce(essentials_niche,'') as essentialsNiche,coalesce(essentials_infrequent,'') as essentialsInfrequent,coalesce(essentials_marginal,'') as essentialsMarginal, \
				coalesce(high_price,'') as highPrice,coalesce(mid_price,'') as midPrice,coalesce(low_price,'') as lowPrice,coalesce(bulk,'') as bulk,coalesce(st_foodtogo,'') as stFoodtogo, \
				coalesce(ambient_drinks,'') as ambientDrinks, coalesce(ambient_food,'') as ambientFood,coalesce(baby,'') as baby,coalesce(bakery,'') as bakery,coalesce(bws,'') as bws, \
				coalesce(confectionery_cakes,'') as confectioneryCakes,coalesce(dairy_milk_cream,'') as dairyMilkCream,coalesce(dairy_other,'') as dairyOther,coalesce(fresh_meals_deli,'') as freshMealsDeli, \
				coalesce(fresh_mfp,'') as freshMfp,coalesce(fresh_produce,'') as freshProduce,coalesce(frozen,'') as frozen,coalesce(health_beauty,'') as healthBeauty,coalesce(home_leisure,'') as homeLeisure, \
				coalesce(household_cleaning,'') as householdCleaning,coalesce(pet_cat,'') as petCat,coalesce(pet_dog,'') as petDog,coalesce(pet_other,'') as petOther,coalesce(fineline_nbr,'') as PPGNumber, \
				coalesce(fineline_desc,'') as PPGDescription,coalesce(mdse_subcatg_nbr,'') as MDSESubcatgNumber,coalesce(mdse_subcatg_desc,'') as MDSESubcatgDescription, \
				coalesce(mdse_catg_nbr,'') as MDSECatgNumber,coalesce(mdse_catg_desc,'') as MDSECatgDescription FROM {}.{} ".format(self.source_db, self.source_table))

        extract_df = extract_df.withColumn("name", F.regexp_replace(F.col("name"), "[\n\r]", " ")).withColumn('lastUpdatedDate',F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
        extract_df.write.mode('overwrite').option("header","true").option("delimiter","|").csv(self.file_path + '/test_Product_Lookup.csv/')
        print("Writing Product_Lookup csv in ... " + self.file_path)

class fileSender:

    def __init__(self):
        self.file_read_path = str(cnt["run_path"]["root_path"]) + str(cnt["file_path"]["CDP"]["output"])

    def file_send(self):


        # **** read from dir prt_files & copy to dir seg_files ****

        # COPY from HDFS to ADLS

        # HDFS FileSystem
        fs_path = hdfs_path

        # blob storage variables
        storageAccount = "4fbd3ac440stg"
        sas_token = "?sv=2020-08-04&ss=bf&srt=c&sp=rwltfx&se=2022-10-28T00:13:57Z&st=2021-10-28T16:13:57Z&spr=https&sig=2Ngcv6Dibn3f3qnPTYaemjPW0ls0d4chEbr6VewU%2FZ4%3D"
        blobContainer = "cdp"
        #folder_filepath = "in/segments"
        azure_blob_fileoutstr = "wasbs://" + blobContainer + "@" + storageAccount + ".blob.core.windows.net/" #+ folder_filepath
        print("azure BLOB path is " + azure_blob_fileoutstr)


       # adls configuration
        configadls_storageAccount = "fs.azure.sas." + blobContainer + "." + storageAccount + ".blob.core.windows.net"
        jvm_conf.set(configadls_storageAccount, sas_token)
        # Getting File System reference fs FROM - path().getFileSystem method
        azure_fs = fs_path(azure_blob_fileoutstr).getFileSystem(jvm_conf)

        srcfs = hdfs_fs
        dstfs = azure_fs

        # delete any existing files in the ADLS directory, before the cloning run
       # file_list_dir_adls = dstfs.listStatus(fs_path(azure_blob_fileoutstr))
       # for file_del in file_list_dir_adls:
        #    filenm_del = file_del.getPath().getName()
        #    print("deleting file ... " + filenm_del + " from " + azure_blob_fileoutstr)
        #    dstfs.delete(fs_path(azure_blob_fileoutstr + "/" + filenm_del))
        #    print("deleted file from BLOB..." + azure_blob_fileoutstr + "/" + filenm_del)

        # read_source_path
        hdfs_dir_readcopyfrom = self.file_read_path

        if srcfs.exists(fs_path(hdfs_dir_readcopyfrom)):
            file_list_dir_hdfs = srcfs.listStatus(fs_path(hdfs_dir_readcopyfrom))
            print("reading..copy from " + hdfs_dir_readcopyfrom)

            # foreach file in report source directory
            for file_cp in file_list_dir_hdfs:
                filenm_copy = file_cp.getPath().getName()
                if not (filenm_copy == "_SUCCESS"):
                    srcpath = fs_path(hdfs_dir_readcopyfrom + filenm_copy)
                    dstpath = fs_path(azure_blob_fileoutstr)

                    fileutil.copy(srcfs, srcpath, dstfs, dstpath, False, jvm_conf)
                    print("file copied... " + filenm_copy + " from " + hdfs_dir_readcopyfrom + " to " + azure_blob_fileoutstr)
            # end for file_list_dir_prt

        print("********************* SPARK JOB COMPLETED *********************")

        # endif


ts = CDP()
ts.file_run()

iw_ext_snd = fileSender()
iw_ext_snd.file_send()

