from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt
from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType, StringType, TimestampType, IntegerType
from pyspark.sql.window import *
from cip.cip.framework.files.generic_processes import purge_data
import subprocess


class transID:
    def __init__(self):
        """
        Assigning Variable from config file
        """
        # Target Table
        self.target_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        #self.target_table = cnt["Customer_Staging_Mart"]["cdd_odl_tables"]["cdd_odl_transaction_ids"]
        self.target_table = 'cdd_odl_transaction_ids_uat_vk930'

        # Source Databases
        self.staging_db = cnt["Customer_Staging_Mart"]["csm_database"]["landing_database"]
        self.information_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        self.idt_db = 'gb_mb_store_secured_dl_tables'

        # Src Tables
        self.store_visit = cnt["Customer_Staging_Mart"]["csm_tables"]["cdd_raw_store_visit"]
        # self.store_visit = 'cdd_raw_store_visit'
        self.scan = cnt["Customer_Staging_Mart"] ["csm_tables"] ["fact_store_visit_scan"]
        # self.scan = 'cdd_raw_store_visit_scan'
        self.store_trans = cnt["Customer_Staging_Mart"] ["csm_tables"] ["fact_store_transaction"]
        # self.store_trans = 'cdd_raw_store_transaction'
        self.store_tender = cnt["Customer_Staging_Mart"] ["csm_tables"] ["cdd_raw_store_visit_tender"]
        # self.store_tender = 'cdd_raw_store_visit_tender'
        self.eic = cnt["Customer_Staging_Mart"] ["csm_tables"] ["eic_order_details"]
        #  self.eic = 'cdd_raw_eic_order_details'
        self.ghs = cnt["Customer_Staging_Mart"] ["csm_tables"] ["cdd_raw_ghs_order"]
        #  self.ghs = 'cdd_raw_ghs_order'
        self.sng = cnt["Customer_Staging_Mart"] ["csm_tables"] ["cdd_raw_sng_visit"]
        #  self.sng = 'cdd_raw_sng_visit'
        self.sng_matching = cnt["Customer_Staging_Mart"] ["cdd_odl_tables"] ["cdd_odl_sng_matching"]
        #  self.sng_matching = 'cdd_odl_sng_matching'
        self.wallet = 'cdd_raw_wallet_pos_txns'
        self.loyalty = 'cdd_raw_loyalty_acct'
        self.grocery_order = 'cdd_raw_ft_grocery_order'
        self.idt_store_tender = 'store_visit_tender'

        # purge
        self.target_partition_clm = cnt["partition"]["transaction"]["partition_column"]
        self.target_partition_val = cnt["partition"]["transaction"]["partition_value"]

        # FOR DAILY LOAD
        self.highwatermark_path = cnt["highwatermark"]["transaction_ids_dir"]
        self.run_path = cnt["run_path"]["root_path"]
        self.highwatermark_raw = \
            spark.read.csv(str(self.run_path) + str(self.highwatermark_path)).rdd.flatMap(lambda x: x).collect()[0]
        self.highwatermark_int1 = datetime.strptime(self.highwatermark_raw, '%Y-%m-%d')
        self.highwatermark_int2 = self.highwatermark_int1 - timedelta(days=2)
        self.highwatermark = self.highwatermark_int2.strftime("%Y-%m-%d")

        print(self.highwatermark)


    def run(self):
        # Ingest data from hive tables and store as df for later transformation and joins

        # assign max/min date to help with historical data load

        sv_df = spark.sql("SELECT visit_dt, visit_nbr, store_nbr, visit_ts, trans_nbr, reg_nbr, visit_subtype_cd, tot_visit_amt, CASE WHEN tot_visit_amt < 0 THEN 'R' ELSE 'S' END AS trans_type \
                                                                        FROM {}.{} WHERE visit_dt > '{}' ".format(
                                                                self.staging_db, self.store_visit, self.highwatermark))
        sv_df.repartition(500)

        # Filter dates to match date range of store_visit. This helps speed of processing data in getNonScanFlag method below.
        sv_max_date = sv_df.agg(F.max('visit_dt')).collect()[0][0]
        sv_min_date = sv_df.agg(F.min('visit_dt')).collect()[0][0]

        svs_df = spark.sql(
            "SELECT visit_dt, store_nbr, visit_nbr FROM {}.{} WHERE visit_dt >= '{}' AND visit_dt <= '{}' ".format(
                self.staging_db, self.scan, sv_min_date, sv_max_date))

        s_trns_df = spark.sql("SELECT visit_date, visit_nbr, store_nbr, tc_nbr, receipt_seq_nbr FROM {}.{}\
                                                                 WHERE visit_date >= '{}' AND visit_date <= '{}' ".format(self.staging_db, self.store_trans, sv_min_date, sv_max_date))

        s_trns_df = s_trns_df.withColumn("row_num", F.row_number().over(
            Window.partitionBy("visit_date", "store_nbr", "visit_nbr").orderBy(F.asc("receipt_seq_nbr"))))
        s_trns_df = s_trns_df.filter(F.col('row_num') == 1).drop('row_num')
        s_trns_df = s_trns_df.select('visit_date', 'store_nbr', 'visit_nbr', 'tc_nbr')
        s_trns_df.repartition(500)

        #RP change to add acct_nbr back in 
        s_tndr_df = spark.sql("SELECT store_nbr, visit_dt, visit_nbr, tndr_amt, debit_rq_tm, acct_nbr, token_id as tkn_id FROM \
                                                                {}.{} WHERE tndr_type_cd = 8 AND visit_dt >= '{}' AND visit_dt <= '{}' ".format(self.staging_db, self.store_tender, sv_min_date, sv_max_date))


        eic_df = spark.sql("SELECT order_id, tc_num FROM {}.{}".format(self.staging_db, self.eic))

        lt_df = spark.sql("select xref_token_id,surrogate_token_id from gb_mb_store_secured_dl_tables.xref_surrogate_token")

        #RP CHANGE to join xref mapping table now and merge columns 
        s_tndr_df = s_tndr_df.join(lt_df, on[s_tndr_df.acct_nbr == lt_df.xref_surrogate_token], how='Left')
        s_tndr_df = s_tndr_df.withColumn('tkn_id', F.when(F.col('tkn_id').isNull(), F.col('surrogate_token_id').otherwise(F.col('tkn_id').drop('surrogate_token_id', 'xref_surrogate_token')
        
        ##do we need to also join the other way and populate acct_nbr with xref_surrogate_token? that way we have picked up all possible data from mapping table, uncomment two lines below if you agree 
        #s_tndr_df = s_tndr_df.join(lt_df, on[s_tndr_df.tkn_id == lt_df.surrogate_token_id], how='Left')
        #s_tndr_df = s_tndr_df.withColumn('acct_nbr', F.when(F.col('acct_nbr').isNull(), F.col('xref_surrogate_token').otherwise(F.col('acct_nbr').drop('surrogate_token_id', 'xref_surrogate_token')

        ghs_df = spark.sql(
            "SELECT singl_profl_id, web_order_id, xprs_order_ind, fulfmt_type_cd, src_last_modfd_ts FROM {}.{}".format(self.staging_db,
                                                                                                    self.ghs))

        sng_df = spark.sql("SELECT trim(cust_singl_profl_id) as cust_singl_profl_id, trim(cart_id) as sngv_cart_id, trim(trans_cd) as trans_cd, trim(trans_nbr) as trans_nbr FROM {}.{}\
                                                 WHERE trim(trans_cd) != '' ".format(self.staging_db, self.sng))
        sng_unq_df = sng_df.groupBy('trans_cd').agg(F.count('cust_singl_profl_id').alias('cnt_spid')).filter(
            F.col('cnt_spid') == 1)
        sng_unq_df = sng_df.join(sng_unq_df, on=['trans_cd'], how='INNER')


        sng_dup_df = sng_df.groupby('trans_cd').agg(F.count('cust_singl_profl_id').alias('cnt_spid')).filter(
            F.col('cnt_spid') > 1)
        sng_dup_df = sng_df.join(sng_dup_df, on=['trans_cd'], how='INNER')

        sng_mtchng_df = spark.sql("SELECT store_nbr, visit_dt, visit_nbr, cart_id as sng_mtch_cart_id FROM {}.{} WHERE \
                                                                visit_dt >= '{}' AND visit_dt <= '{}' ".format(
            self.information_db, self.sng_matching, sv_min_date, sv_max_date))

        wallet_df = spark.sql("SELECT wallet_id, reg_nbr, store_nbr, trans_rcpt_nbr, visit_dt, trans_nbr, chnl_nm FROM {}.{} " \
                                                                       .format(self.staging_db, self.wallet, sv_min_date, sv_max_date))
        wallet_ecom_df = wallet_df.filter(F.col('chnl_nm') == 'ecom')\
                                    .select(F.col('wallet_id').alias('ecom_wallet_id'), F.col('trans_rcpt_nbr'))

        wallet_non_ecom_df = wallet_df.filter(F.col('chnl_nm') == 'store')\
                                    .select(F.col('wallet_id').alias('non_ecom_wallet_id'), F.col('store_nbr'), F.col('visit_dt'), F.col('reg_nbr'), F.col('trans_nbr'))\
                                    .repartition(500)

        loyalty_df = spark.sql("SELECT wallet_id, singl_profl_id FROM {}.{} ".format(self.staging_db, self.loyalty))
        wallet_loyalty = wallet_df.join(loyalty_df, on=["wallet_id"], how='LEFT')\
                                    .select(F.col('wallet_id'), F.col('singl_profl_id').alias('loyalty_spid'))


        # Add derived basket_id to store_visit, scan, store_tender and sng_matching

        sv_df = sv_df.withColumn("basket_id", F.concat((F.col("visit_dt").cast(StringType())), (F.col("store_nbr")),
                                                       (F.col("visit_nbr"))))
        sv_df = sv_df.withColumn("basket_id", F.regexp_replace('basket_id', '-', '').cast(DecimalType(38, 0)))

        svs_df = svs_df.withColumn("svs_bas_id", F.concat((F.col("visit_dt").cast(StringType())), (F.col("store_nbr")),
                                                          (F.col("visit_nbr"))))
        svs_df = svs_df.withColumn("svs_bas_id", F.regexp_replace('svs_bas_id', '-', '').cast(DecimalType(38, 0)))

        s_trns_df = s_trns_df.withColumn("basket_id",
                                         F.concat((F.col("visit_date").cast(StringType())), (F.col("store_nbr")),
                                                  (F.col("visit_nbr"))))
        s_trns_df = s_trns_df.withColumn("basket_id", F.regexp_replace('basket_id', '-', '').cast(DecimalType(38, 0)))

        sng_mtchng_df = sng_mtchng_df.withColumn("basket_id",
                                                 F.concat((F.col("visit_dt").cast(StringType())), (F.col("store_nbr")),
                                                          (F.col("visit_nbr"))))
        sng_mtchng_df = sng_mtchng_df.withColumn("basket_id",
                                                 F.regexp_replace('basket_id', '-', '').cast(DecimalType(38, 0)))

        #Populate order_id for converted stores
        df_idt_store_visit = spark.sql("SELECT store_nbr, min(visit_dt) as ncr_rollout_dt FROM {}.{} WHERE visit_dt >= '2023-09-01' and token_id is not null GROUP BY store_nbr".format(self.idt_db, self.idt_store_tender))

        grocery_order_df = spark.sql("SELECT account_number, order_id, store_id, actual_delivery_date, actual_delivery_time, epos_total FROM {}.{}  where actual_delivery_date >= '{}' AND actual_delivery_date <= '{}' ".format(self.staging_db, self.grocery_order, sv_min_date, sv_max_date))

        grocery_ncr_df = grocery_order_df.join(df_idt_store_visit, on=[grocery_order_df.store_id == df_idt_store_visit.store_nbr,
                                                                       grocery_order_df.actual_delivery_date >= df_idt_store_visit.ncr_rollout_dt])

        grocery_sv_df = grocery_ncr_df.join(sv_df, on=[sv_df.store_nbr == grocery_ncr_df.store_id,
                                                       sv_df.visit_dt == grocery_ncr_df.actual_delivery_date,
                                                       sv_df.tot_visit_amt == grocery_ncr_df.epos_total])

        grocery_sv_df = grocery_sv_df.withColumn("sv_visit_ts", F.to_timestamp(F.col("visit_ts"))) \
                                .withColumn("grocery_visit_ts", F.to_timestamp(F.col("actual_delivery_time"))) \
                                .withColumn("difference", F.abs(F.col("sv_visit_ts").cast("long") - F.col("grocery_visit_ts").cast("long")))

        grocery_sv_df = grocery_sv_df.join(grocery_sv_df.groupBy('order_id', 'actual_delivery_date').agg(F.min('difference').alias('min_time')), on = ['order_id', 'actual_delivery_date'])
        grocery_sv_df = grocery_sv_df.withColumn("row_num", F.row_number().over(Window.partitionBy("store_id", "actual_delivery_date", "epos_total", "visit_nbr").orderBy(F.asc("difference"))))
        grocery_sv_df = grocery_sv_df.filter(F.col('row_num') == 1)

        grocery_df = grocery_sv_df.select(F.col('order_id').alias('grcry_order_id'), F.col('account_number').alias('grcry_spid'), F.col('basket_id'), F.col('ncr_rollout_dt'), F.col('store_id').alias('store_nbr'))
        grocery_df = grocery_df.dropDuplicates(['grcry_order_id'])
        ## CALL FUNCTIONS ##

        s_tndr_df = self.getAcctNbr(s_tndr_df)
        sv_df = self.getNonScanFlag(sv_df, svs_df)
        sv_df = self.getCSPID(sv_df, s_trns_df, sng_unq_df, sng_dup_df)
        eic_ghs_trans_df = self.getOrderID(s_trns_df, eic_df, ghs_df)
        eic_ghs_trans_df = eic_ghs_trans_df.dropDuplicates(['order_id'])



        trans_ids_df = self.CreateOutput(sv_df, eic_ghs_trans_df, s_tndr_df, lt_df, sng_mtchng_df, wallet_ecom_df, wallet_non_ecom_df, wallet_loyalty, grocery_df)


        trans_ids_df.printSchema()
        final_df = self.FinalDF(trans_ids_df)
        final_df.printSchema()
        # write to table
        #final_df.write.mode("overwrite").saveAsTable("{}.{}".format(self.target_db, self.target_table))


        # FOR DAILY LOAD
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        #final_df.write.insertInto("{}.{}".format(self.target_db, self.target_table), overwrite = True)

        final_df.write.mode("overwrite").insertInto("{}.{}".format(self.target_db, self.target_table), overwrite=True)




    @staticmethod
    def getAcctNbr(s_tndr_df): #added acct_nbr back in here RP 
        s_tndr_df = s_tndr_df.withColumn("row_num", F.row_number().over(
            Window.partitionBy("visit_dt", "store_nbr", "visit_nbr").orderBy(F.desc("tndr_amt"))))
        s_tndr_df = s_tndr_df.filter(F.col('row_num') == 1).drop('row_num')
        s_tndr_df = s_tndr_df.withColumn("row_num", F.row_number().over(
            Window.partitionBy("visit_dt", "store_nbr", "visit_nbr", "tndr_amt").orderBy(F.asc("debit_rq_tm"))))
        s_tndr_df = s_tndr_df.filter(F.col('row_num') == 1).drop('row_num')
        s_tndr_df = s_tndr_df.select('visit_dt', 'store_nbr', 'visit_nbr', 'acct_nbr','tkn_id')
        s_tndr_df.repartition(500)

        return (s_tndr_df)

    @staticmethod
    def getNonScanFlag(sv_df, svs_df):
        sv_df = sv_df.join(svs_df, on=[sv_df.basket_id == svs_df.svs_bas_id], how='LEFT').drop(svs_df.visit_dt).drop(
            svs_df.store_nbr).drop(svs_df.visit_nbr)
        sv_df = sv_df.withColumn('non_scan_visit_ind', F.expr("CASE WHEN svs_bas_id IS NULL THEN '1'" +
                                                              "ELSE '0' END"))
        return (sv_df)

    @staticmethod
    def getCSPID(sv_df, s_trns_df, sng_unq_df, sng_dup_df):


        sv_df = sv_df.join(s_trns_df, on=['basket_id'], how='LEFT').drop(s_trns_df.visit_nbr).drop(
            s_trns_df.store_nbr).drop(s_trns_df.visit_date)

        sv_df = sv_df.withColumn('tc_nbr_str', sv_df.tc_nbr.cast(StringType()))

        # from the two dataframes created in the main program, fetch the correct SPID and sngv_cart_id -- if the SPID is distinct then match on tc_nbr = trans_cd BUT if SPID is a duplicate match on trans_nbr = trans_nbr as well ##

        sng_unq_df = sng_unq_df.withColumn('trans_cd_unstr', sng_unq_df.trans_cd.cast(StringType())).select('trans_cd', F.col('cust_singl_profl_id').alias('unq_spid'),
                                       F.col('sngv_cart_id').alias('unq_cart_id'), 'trans_cd_unstr')


        sv_df = sv_df.join(sng_unq_df, on=[(sv_df.tc_nbr_str == sng_unq_df.trans_cd_unstr)], how='LEFT')


        sng_dup_df = sng_dup_df.withColumn('trans_cd_str', sng_dup_df.trans_cd.cast(StringType())).select(F.col('trans_nbr').alias('dup_tnbr'), 'trans_cd',
                                       F.col('cust_singl_profl_id').alias('dup_spid'),
                                       F.col('sngv_cart_id').alias('dup_cart_id'), 'trans_cd_str')


        sv_df = sv_df.join(sng_dup_df,
                           on=[(sv_df.tc_nbr_str == sng_dup_df.trans_cd_str) & (sv_df.trans_nbr == sng_dup_df.dup_tnbr)],
                           how='LEFT')

        sv_df = sv_df.withColumn('cust_singl_profl_id',
                                 F.expr("CASE WHEN dup_spid IS NOT NULL THEN dup_spid ELSE unq_spid END"))
        sv_df = sv_df.withColumn('sngv_cart_id',
                                F.expr("CASE WHEN dup_cart_id IS NOT NULL THEN dup_cart_id ELSE unq_cart_id END"))



        sv_df = sv_df.select('basket_id', 'visit_dt', 'store_nbr', 'visit_nbr', 'visit_ts', 'trans_nbr', 'reg_nbr',
                             'visit_subtype_cd', 'trans_type', 'tc_nbr', 'cust_singl_profl_id', 'sngv_cart_id', 'non_scan_visit_ind') \
           .drop(s_trns_df.tc_nbr)

        sv_df.repartition(500)

        return (sv_df)

    @staticmethod
    def getOrderID(s_trns_df, eic_df, ghs_df):

        ghs_df = ghs_df.withColumn("row_num", F.row_number().over(Window.partitionBy("web_order_id").orderBy(F.desc("src_last_modfd_ts"))))
        ghs_df = ghs_df.filter(F.col('row_num') == 1).drop('row_num').drop('src_last_modfd_ts')
        eic_ghs_df = eic_df.join(ghs_df, on=[eic_df.order_id == ghs_df.web_order_id], how='LEFT')
        eic_ghs_trans_df = s_trns_df.join(eic_ghs_df, on=[s_trns_df.tc_nbr == eic_ghs_df.tc_num], how='LEFT').drop(
            s_trns_df.visit_nbr).drop(s_trns_df.store_nbr).drop(s_trns_df.visit_date)

        return (eic_ghs_trans_df)

    @staticmethod
    def CreateOutput(sv_df, eic_ghs_trans_df, s_tndr_df, lt_df, sng_mtchng_df, wallet_ecom_df, wallet_non_ecom_df, wallet_loyalty, grocery_df):
        # Variables used for channel_id derivation
        reg_nbr = 82
        subtype_li = [89, 152, 201, 202]
        def_channel_id = 1

        final_df = sv_df.join(eic_ghs_trans_df, on=["basket_id"], how='LEFT').drop(eic_ghs_trans_df.basket_id)
        final_df = final_df.withColumn("channel_id", (
            F.when(F.col('reg_nbr') == reg_nbr, 2).when(F.col('visit_subtype_cd').isin(subtype_li), 3).otherwise(
                def_channel_id)))
        final_df = final_df.join(s_tndr_df, on=["visit_dt", "store_nbr", "visit_nbr"], how='LEFT').drop(
            s_tndr_df.visit_dt).drop(s_tndr_df.store_nbr).drop(s_tndr_df.visit_nbr)
        ************final_df = final_df.join(lt_df,on=[final_df.tkn_id == lt_df.surrogate_token_id],how='LEFT')***** remove 
        #start of populate order_id to final_df
        final_df = final_df.join(grocery_df, on=["store_nbr","basket_id"], how='LEFT')
        final_df = final_df.withColumn('fnl_order_id', F.expr("CASE WHEN grcry_order_id IS NOT NULL THEN grcry_order_id ELSE order_id END"))
        final_df = final_df.withColumn('fnl_spid', F.expr("CASE WHEN grcry_spid IS NOT NULL THEN grcry_spid ELSE singl_profl_id END"))
        #end
        final_df = final_df.join(wallet_ecom_df, on=[final_df.fnl_order_id == wallet_ecom_df.trans_rcpt_nbr], how='LEFT').drop(wallet_ecom_df.trans_rcpt_nbr)
        final_df = final_df.join(wallet_non_ecom_df, on=["store_nbr", "visit_dt", "reg_nbr", "trans_nbr"], how='LEFT')\
                                    .drop(wallet_non_ecom_df.reg_nbr)\
                                    .drop(wallet_non_ecom_df.store_nbr)\
                                    .drop(wallet_non_ecom_df.visit_dt)\
                                    .drop(wallet_non_ecom_df.trans_nbr)

        final_df = final_df.withColumn('wallet_id', F.expr("CASE WHEN ecom_wallet_id IS NOT NULL THEN ecom_wallet_id ELSE non_ecom_wallet_id END"))
        final_df = final_df.join(wallet_loyalty, on=[final_df.wallet_id == wallet_loyalty.wallet_id], how='LEFT').drop(wallet_loyalty.wallet_id)
        trans_ids_df = final_df.join(sng_mtchng_df, on=["basket_id"], how='LEFT').drop(sng_mtchng_df.visit_dt).drop(
            sng_mtchng_df.store_nbr).drop(sng_mtchng_df.visit_nbr)
        return (trans_ids_df)

    @staticmethod
    def FinalDF(trans_ids_df):
       # Dates used for cart_id
        before_date = '2021-12-06'
        after_date = '2021-12-05'

        final_df = trans_ids_df.select('basket_id',\
                                       'store_nbr',\
                                       'visit_nbr',\
                                       'visit_ts', \
                                       'trans_nbr', \
                                       'reg_nbr', \
                                       'visit_subtype_cd',\
                                       'trans_type',\
                                       'channel_id', 'tc_nbr',\
                                        (F.col('acct_nbr').alias('lead_xref')), \ #added acct_nbr back in here 
                                        #(F.col('fnl_order_id').alias('ghs_order_id')),\
                                        F.when((F.col('channel_id') == 2), trans_ids_df.fnl_order_id).otherwise(None).alias('ghs_order_id'), \
                                        F.when((F.col('channel_id') == 2) & (F.length(F.col('fnl_order_id')) == 18), 'Marketplace')\
                                            .when((F.col('channel_id') == 2) & (F.col('fnl_order_id').isNull()) , 'Unknown')\
                                            .when((F.col('channel_id') == 2) & (F.length(F.col('fnl_order_id')) != 18) & ((F.col('fnl_order_id').isNotNull())), trans_ids_df.fulfmt_type_cd).otherwise(None).alias('fulfmt_type_cd'), \
                                        'xprs_order_ind', \
                                        F.when(F.col('wallet_id').isNotNull(), trans_ids_df.loyalty_spid)\
                                            .when((F.col('channel_id') == 2) & (F.col('wallet_id').isNull()), trans_ids_df.fnl_spid)\
                                            .when((F.col('channel_id') == 3) & (F.col('wallet_id').isNull()), trans_ids_df.cust_singl_profl_id)\
                                            .otherwise(None).alias('singl_profl_id'), \
                                        F.when(((F.col('channel_id') == 3) & (trans_ids_df.visit_dt > (after_date))), trans_ids_df.sngv_cart_id).otherwise(None).alias('sng_cart_id'), \
                                        'wallet_id', \
                                        'non_scan_visit_ind', \
                                        (F.col('tkn_id').alias('lead_token_id')), \
                                        'visit_dt')

        final_df = final_df.distinct()
        final_df.repartition(500)
        return (final_df)


ts = transID()
ts.run()
