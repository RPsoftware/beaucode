from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt
from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql.types import  DecimalType


class pos_seg_choices:
    def __init__(self):
        """
        Assigning Variable from config file
        """
        # Target Table
        self.target_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        #self.target_table = cnt["Customer_Staging_Mart"]["cdd_odl_tables"]["cdd_odl_pos_seg_choices"]
        # Databases
        self.staging_db = cnt["Customer_Staging_Mart"]["csm_database"]["landing_database"]
        self.information_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        # Config Table
        #self.config_choices_table = cnt["Customer_Staging_Mart"]["cdd_odl_tables"]["cdd_odl_pos_seg_choices_config_coefficients"]
        # Src Tables
        self.trans_dept = cnt["Customer_Staging_Mart"]["cdd_odl_tables"]["cdd_odl_pos_transaction_dept"]
        #self.mission = cnt["Customer_Staging_Mart"] ["cdd_odl_tables"] ["cdd_odl_pos_seg_mission"]

    def run(self):
        #1 - pull data from trans_dept into df and filter by date
        trans_dept_df = spark.sql("SELECT * FROM gb_customer_data_domain_odl.cdd_odl_pos_transaction_dept where visit_dt = '2018-11-01'")
        config_df = spark.sql("SELECT coefficient_id, coefficient FROM gb_customer_data_domain_odl.cdd_odl_pos_seg_choices_config_coefficients")
        trips_df = spark.sql("SELECT store_nbr, visit_dt, visit_nbr, triptype_id FROM gb_customer_data_domain_odl.cdd_odl_pos_seg_trip")
        mission_df = spark.sql("SELECT store_nbr, visit_dt, visit_nbr, low_mission_id FROM gb_customer_data_domain_odl.cdd_odl_pos_seg_mission")
        trans_mis_trips_df = self.getdata(trans_dept_df, mission_df, trips_df)
        coeff = self.getcoeff(config_df)
        stage_2_df = self.getstage2(trans_mis_trips_df, coeff)
        stage_3_df = self.getstage3(stage_2_df)
        stage_3_df.write.partitionBy('visit_dt').mode("overwrite").saveAsTable("gb_customer_data_domain_odl.cdd_odl_pos_seg_choices")

    #5 - write function to perform calculations on trans_dept. Sum columns first and give _sum alias, then divide and alias with the name given in requirements
    @staticmethod
    def getdata (trans_dept_df, mission_df, trips_df):


         trans_stage1_df  = spark.sql('''SELECT store_nbr, visit_nbr, visit_dt, MAX(load_ts) AS load_ts, CAST(SUM(bulk) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS bulk,
                CAST(SUM(high_price) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS pr_high, CAST(SUM(low_price) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS pr_low,
                CAST(SUM(mid_price) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS pr_mid,
                CAST(SUM(own_label_smartprice) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_asda_smartprice,
                CAST(SUM(own_label_extraspecial) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_asda_extraspecial,
                CAST(SUM(own_label_goodforyou) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_asda_goodforyou,
                CAST(SUM(own_label_freefrom) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_asda_freefrom,
                CAST(SUM(own_label_standard) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_asda_standard,
                CAST(SUM(own_label_asdaother) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_asda_other,
                CAST(SUM(own_label) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_own_label,
                CAST(SUM(branded) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_branded,
                CAST(SUM(organic) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS organic,
                CAST(SUM(own_label_choice) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_own_label_choice,
                CAST(SUM(branded_choice) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS br_branded_choice,
                CAST(SUM(essentials_core) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ess_core,
                CAST(SUM(essentials_marginal) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ess_marginal,
                CAST(SUM(essentials_niche) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ess_niche,
                CAST(SUM(essentials_infrequent) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ess_infrequent,
                CAST(SUM(essentials_staples) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ess_staples,
                CAST(SUM(essentials_occasional) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ess_occasional,
                CAST(SUM(baby_new_born) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_baby_newborn,
                CAST(SUM(toddler) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_toddler,
                CAST(SUM(adventurous) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_adventurous,
                CAST(SUM(children) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_children,
                CAST(SUM(convenience_readymeals) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_conv_ready_meals,
                CAST(SUM(convenience_timesaving) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_conv_time_saving,
                CAST(SUM(healthy) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_healthy,
                CAST(SUM(high_calorie) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_high_calorie,
                CAST(SUM(low_calorie) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_low_calorie,
                CAST(SUM(scratch_cooking) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_scratch_cooking,
                CAST(SUM(traditional) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_traditional,
                CAST(SUM(vegetarian) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_vegetarian,
                CAST(SUM(ethical) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS sno_ethical,
                CAST(SUM(st_foodtogo) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ftg_foodtogo,
                CAST(SUM(bws) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_bws,
                CAST(SUM(ambient_drinks) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_ambient_drinks,
                CAST(SUM(health_beauty) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_health_and_beauty,
                CAST(SUM(ambient_food) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_ambient_food,
                CAST(SUM(baby) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_baby,
                CAST(SUM(pet_dog) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_pet_dog,
                CAST(SUM(pet_cat) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_pet_cat,
                CAST(SUM(pet_other) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_pet_other,
                CAST(SUM(household_cleaning) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_household_cleaning,
                CAST(SUM(confectionery_cakes) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_conf_cakes_biscuits,
                CAST(SUM(fresh_meals_deli) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_fresh_meals_deli,
                CAST(SUM(home_leisure) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_home_and_leisure,
                CAST(SUM(bakery) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_bakery,
                CAST(SUM(fresh_produce) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_fresh_produce,
                CAST(SUM(dairy_other) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_dairy_other,
                CAST(SUM(dairy_milk_cream) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_dairy_milk_cream,
                CAST(SUM(fresh_mfp) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_fresh_meat_fish_poultry,
                CAST(SUM(frozen) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS ac_frozen
                FROM gb_customer_data_domain_odl.cdd_odl_pos_transaction_dept
                WHERE visit_dt >= date_add(current_date(),-100)
                GROUP BY store_nbr, visit_nbr, visit_dt''')

           trans_mis_df = trans_stage1_df.join(mission_df, on=[(trans_stage1_df.store_nbr == mission_df.store_nbr) &
                                                                 (trans_stage1_df.visit_dt == mission_df.visit_dt) & (trans_stage1_df.visit_nbr == mission_df.visit_nbr)], how='LEFT').drop(mission_df.store_nbr).drop(mission_df.visit_dt).drop(mission_df.visit_nbr)
         trans_mis_trips_df = trans_mis_df.join(trips_df, on=[(trans_mis_df.store_nbr == trips_df.store_nbr) & (trans_mis_df.visit_dt == trips_df.visit_dt) & (trans_mis_df.visit_nbr == trips_df.visit_nbr)],
                                               how='LEFT').drop(trips_df.store_nbr).drop(trips_df.visit_dt).drop(trips_df.visit_nbr)

         return trans_mis_trips_df

    @staticmethod
    def getcoeff (config_df):
         coeff_df = config_df.select("coefficient_id", "coefficient")
         coeff = coeff_df.rdd.map(lambda x: (x[0], x[1])).collectAsMap()

         return coeff


    @staticmethod
    def getstage2 (trans_mis_trips_df, coeff):

        stage_2_df = trans_mis_trips_df.select((F.concat((F.date_format(F.col('visit_dt'), "yyyyMMdd")), F.col('store_nbr'), F.col('visit_nbr')).cast(DecimalType(38,0))).alias('basket_id'), "store_nbr", "visit_dt", "visit_nbr", "load_ts", "low_mission_id","triptype_id", F.lit((coeff.get('@ais_c') + ((F.col('bulk') * coeff.get('@ais_b'))) + \
                                                                                        ((F.col('pr_high') * coeff.get('@ais_ph'))) + ((F.col('pr_low') * coeff.get('@ais_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@ais_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@ais_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@ais_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@ais_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@ais_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@ais_snoh'))) + \
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@ais_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@ais_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@ais_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@ais_snoe'))))).alias('authentic_ingredients_sc'), \
                                                                                F.lit((coeff.get('@lhs_c') + ((F.col('bulk') * coeff.get('@lhs_b')))+ \
                                                                                        ((F.col('pr_high') * coeff.get('@lhs_ph'))) + \
                                                                                        ((F.col('pr_low') * coeff.get('@lhs_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@lhs_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@lhs_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@lhs_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@lhs_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@lhs_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@lhs_snoh'))) +
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@lhs_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@lhs_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@lhs_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@lhs_snoe'))))).alias('lowcost_highcal_sc'), \
                                                                                F.lit((coeff.get('@hes_c') + ((F.col('bulk') * coeff.get('@hes_b'))) + \
                                                                                        ((F.col('pr_high') * coeff.get('@hes_ph'))) + \
                                                                                        ((F.col('pr_low') * coeff.get('@hes_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@hes_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@hes_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@hes_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@hes_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@hes_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@hes_snoh'))) + \
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@hes_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@hes_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@hes_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@hes_snoe'))))).alias('healthy_ethical_sc'),
                                                                                F.lit((coeff.get('@ms_c') + ((F.col('bulk') * coeff.get('@ms_b'))) + \
                                                                                    ((F.col('pr_high') * coeff.get('@ms_ph'))) + \
                                                                                        ((F.col('pr_low') * coeff.get('@ms_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@ms_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@ms_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@ms_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@ms_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@ms_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@ms_snoh'))) + \
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@ms_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@ms_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@ms_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@ms_snoe'))))).alias('mainstream_sc'),    F.lit((coeff.get('@clcs_c') + ((F.col('bulk') * coeff.get('@clcs_b'))) + \
                                                                                        ((F.col('pr_high') * coeff.get('@clcs_ph'))) + \
                                                                                        ((F.col('pr_low') * coeff.get('@clcs_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@clcs_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@clcs_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@clcs_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@clcs_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@clcs_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@clcs_snoh'))) + \
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@clcs_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@clcs_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@clcs_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@clcs_snoe'))))).alias('child_led_choices_sc'),
                                                                                F.lit((coeff.get('@bfbs_c') + ((F.col('bulk') * coeff.get('@bfbs_b'))) + \
                                                                                    ((F.col('pr_high') * coeff.get('@bfbs_ph'))) + \
                                                                                        ((F.col('pr_low') * coeff.get('@bfbs_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@bfbs_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@bfbs_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@bfbs_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@bfbs_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@bfbs_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@bfbs_snoh'))) + \
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@bfbs_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@bfbs_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@bfbs_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@bfbs_snoe'))))).alias('buying_for_baby_sc'),
                                                                                F.lit((coeff.get('@sbs_c') + ((F.col('bulk') * coeff.get('@sbs_b'))) + \
                                                                                        ((F.col('pr_high') * coeff.get('@sbs_ph'))) + \
                                                                                        ((F.col('pr_low') * coeff.get('@sbs_pl'))) + \
                                                                                        ((F.col('sno_baby_newborn') * coeff.get('@sbs_snobn'))) + \
                                                                                        ((F.col('sno_toddler') * coeff.get('@sbs_snotod'))) + \
                                                                                        ((F.col('sno_adventurous') * coeff.get('@sbs_snoa'))) + \
                                                                                        ((F.col('sno_children') * coeff.get('@sbs_snoc'))) + \
                                                                                        ((F.col('sno_conv_ready_meals') * coeff.get('@sbs_snocrm'))) + \
                                                                                        ((F.col('sno_healthy') * coeff.get('@sbs_snoh'))) + \
                                                                                        ((F.col('sno_high_calorie') * coeff.get('@sbs_snohc'))) + \
                                                                                        ((F.col('sno_scratch_cooking') * coeff.get('@sbs_snosc'))) + \
                                                                                        ((F.col('sno_traditional') * coeff.get('@sbs_snotra'))) + \
                                                                                        ((F.col('sno_ethical') * coeff.get('@sbs_snoe'))))).alias('stretching_budget_sc'),
                                                                                F.lit((coeff.get('@nbs_c') + ((F.col('br_own_label') * coeff.get('@nbs_bol'))) + \
                                                                                    ((F.col('br_branded') * coeff.get('@nbs_bb'))))).alias('national_brand_sc'),
                                                                                F.lit((coeff.get('@obs_c') + ((F.col('br_own_label') * coeff.get('@obs_bol'))) + \
                                                                                    ((F.col('br_branded') * coeff.get('@obs_bb'))))).alias('own_brand_sc'))
        return stage_2_df

    @staticmethod
    def getstage3(stage_2_df):

        stage_3_df = stage_2_df.select("basket_id", "store_nbr", "visit_nbr", "visit_dt", "load_ts", "triptype_id", "low_mission_id", \
                "authentic_ingredients_sc" , "lowcost_highcal_sc", "healthy_ethical_sc", "mainstream_sc", "child_led_choices_sc", "buying_for_baby_sc", "stretching_budget_sc", "national_brand_sc", "own_brand_sc", \
                F.when(F.col('triptype_id') == 99, 99) \
                .when(F.col(('triptype_id') < 5), 98) \
                .when(F.col(('authentic_ingredients_sc') > (F.col('lowcost_highcal_sc'))) & \
                    (F.col('authentic_ingredients_sc') > (F.col('healthy_ethical_sc'))) & \
                    (F.col('authentic_ingredients_sc') > (F.col('mainstream_sc'))) & \
                    (F.col('authentic_ingredients_sc') > (F.col('child_led_choices_sc'))) & \
                                        (F.col('authentic_ingredients_sc') > (F.col('buying_for_baby_sc'))) & \
                                        (F.col('authentic_ingredients_sc') > (F.col('stretching_budget_sc'))),  4) \
                .when(F.col(('lowcost_highcal_sc') > (F.col('healthy_ethical_sc'))) & \
                                        (F.col('lowcost_highcal_sc') > (F.col('mainstream_sc'))) & \
                                        (F.col('lowcost_highcal_sc') > (F.col('child_led_choices_sc'))) & \
                                        (F.col('lowcost_highcal_sc') > (F.col('buying_for_baby_sc'))) & \
                                        (F.col('lowcost_highcal_sc') > (F.col('stretching_budget_sc'))), 1) \
                .when(F.col(('healthy_ethical_sc') > (F.col('mainstream_sc'))) & \
                    (F.col('healthy_ethical_sc') > (F.col('child_led_choices_sc'))) & \
                                        (F.col('healthy_ethical_sc') > (F.col('buying_for_baby_sc'))) & \
                                        (F.col('healthy_ethical_sc') > (F.col('stretching_budget_sc'))), 3) \
                .when(F.col(('mainstream_sc') > (F.col('child_led_choices_sc'))) & \
                                        (F.col('mainstream_sc') > (F.col('buying_for_baby_sc'))) & \
                                        (F.col('mainstream_sc') > (F.col('stretching_budget_sc'))) & \
                                        (F.col('own_brand_sc') > (F.col('national_brand_sc'))), 8)
                .when(F.col(('mainstream_sc') > (F.col('child_led_choices_sc'))) & \
                                        (F.col('mainstream_sc') > (F.col('buying_for_baby_sc'))) & \
                                        (F.col('mainstream_sc') > (F.col('stretching_budget_sc'))) & \
                                        (F.col('national_brand_sc') > (F.col('own_brand_sc'))), 7)
                .when(F.col((F.col('child_led_choices_sc') > (F.col('buying_for_baby_sc'))) & \
                                        (F.col('child_led_choices_sc') > (F.col('stretching_budget_sc'))), 6)) \
                .when(F.col(('buying_for_baby_sc') > (F.col('stretching_budget_sc'))), 5) \
                .otherwise('2')).alias('choices_id')

        return stage_3_df

ts = pos_seg_choices()
ts.run()








