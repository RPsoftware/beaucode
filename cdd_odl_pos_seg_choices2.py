from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt

class pos_seg_choices:
    def __init__(self):
        """
        Assigning Variable from config file
        """
        # Target Table
        self.target_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        self.target_table = cnt["Customer_Staging_Mart"]["cdd_odl_tables"]["cdd_odl_pos_seg_choices"]
        # Databases
        self.staging_db = cnt["Customer_Staging_Mart"]["csm_database"]["landing_database"]
        self.information_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        #Config Tables
        self.configChoicesCoef_table = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["cdd_odl_pos_seg_choices_coefficients"]
        #scTable
        self.src_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["cdd_odl_pos_transaction_dept"]

    def run(self):
        print("****************Execution Start****************")

        df_sum_values = spark.sql( '''SELECT store_nbr, visit_nbr, visit_date, MAX(load_ts) AS load_ts, 
                                   CAST(SUM(bulk) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS bulk,
                                   CAST(SUM(high_price) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS pr_high,
                                   CAST(SUM(low_price) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS pr_low, 
                                   CAST(SUM(mid_price) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS pr_mid,
                                   CAST(SUM(own_label_smartprice) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_asda_smartprice, 
                                   CAST(SUM(own_label_extraspecial) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_asda_extraspecial, 
                                   CAST(SUM(own_label_goodforyou) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_asda_goodforyou,
                                   CAST(SUM(own_label_freefrom) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_asda_freefrom,
                                   CAST(SUM(own_label_standard) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_asda_standard,
                                   CAST(SUM(own_label_asdaother) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_asda_other, 
                                   CAST(SUM(own_label) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_own_label, 
                                   CAST(SUM(branded) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_branded, 
                                   CAST(SUM(organic) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS organic, 
                                   CAST(SUM(own_label_choice) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_own_label_choice, 
                                   CAST(SUM(branded_choice) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS br_branded_choice, 
                                   CAST(SUM(essentials_core) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ess_core, 
                                   CAST(SUM(essentials_marginal) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ess_marginal, 
                                   CAST(SUM(essentials_niche) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ess_niche, 
                                   CAST(SUM(essentials_infrequent) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ess_infrequent,
                                   CAST(SUM(essentials_staples) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ess_staples, 
                                   CAST(SUM(essentials_occasional) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ess_occasional,
                                   CAST(SUM(baby_new_born) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_baby_newborn,
                                   CAST(SUM(toddler) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_toddler,
                                   CAST(SUM(adventurous) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_adventurous,
                                   CAST(SUM(children) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_children,
                                   CAST(SUM(convenience_readymeals) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_conv_ready_meals,
                                   CAST(SUM(convenience_timesaving) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_conv_time_saving,
                                   CAST(SUM(healthy) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_healthy,
                                   CAST(SUM(high_calorie) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_high_calorie,
                                   CAST(SUM(low_calorie) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_low_calorie,
                                   CAST(SUM(scratch_cooking) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_scratch_cooking,
                                   CAST(SUM(traditional) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_traditional,
                                   CAST(SUM(vegetarian) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_vegetarian,
                                   CAST(SUM(ethical) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS sno_ethical,
                                   CAST(SUM(st_foodtogo) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ftg_foodtogo,
                                   CAST(SUM(bws) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_bws,
                                   CAST(SUM(ambient_drinks) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_ambient_drinks,
                                   CAST(SUM(health_beauty) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_health_and_beauty,
                                   CAST(SUM(ambient_food) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_ambient_food,
                                   CAST(SUM(baby) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_baby,
                                   CAST(SUM(pet_dog) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_pet_dog,
                                   CAST(SUM(pet_cat) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_pet_cat,
                                   CAST(SUM(pet_other) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_pet_other,
                                   CAST(SUM(household_cleaning) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_household_cleaning, 
                                   CAST(SUM(confectionery_cakes) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_conf_cakes_biscuits,
                                   CAST(SUM(fresh_meals_deli) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_fresh_meals_deli, 
                                   CAST(SUM(home_leisure) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_home_and_leisure,
                                   CAST(SUM(bakery) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_bakery,
                                   CAST(SUM(fresh_produce) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_fresh_produce,
                                   CAST(SUM(dairy_other) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_dairy_other,
                                   CAST(SUM(dairy_milk_cream) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_dairy_milk_cream,
                                   CAST(SUM(fresh_mfp) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_fresh_meat_fish_poultry,
                                   CAST(SUM(frozen) AS DECIMAL(10,5)) / CAST(SUM(dist_items) AS DECIMAL(10,5)) AS ac_frozen FROM {}.{}'''.format(self.staging_db, self.src_db))

        df_sum_values.createOrReplaceTempView("pos_seg_sum_values")
        temp_df = ('select * from pos_seg_sum_values')
        temp_df.show()

        print("************** SPARK JOB complete****************")

ts = pos_seg_choices()
ts.run()