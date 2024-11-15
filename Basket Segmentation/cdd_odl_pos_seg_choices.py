from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt

class pos_seg_choices:
    def __init__(self):
        """
        Assigning Variable from config file
        """
        # Target Table
        #self.target_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        #self.target_table = cnt["Customer_Staging_Mart"]["cdd_odl_tables"]["cdd_odl_pos_seg_choices"]
        # Databases
        #self.staging_db = cnt["Customer_Staging_Mart"]["csm_database"]["landing_database"]
        #self.information_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["information_database"]
        #Config Tables
        #self.configChoicescoeff_table = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["cdd_odl_pos_seg_choices_coefficients"]
        #scTable
        #self.src_db = cnt["Customer_Staging_Mart"]["cdd_odl_database"]["cdd_odl_pos_transaction_dept"]

    def run(self):
        print("****************Execution Start****************")

       #get data from pos_transactions_dept from previous 3 years, group by store_nbr,visit_nbr,visit_dt and sum values
        df_sum_values = spark.sql('''
                SELECT store_nbr, visit_nbr, visit_dt, MAX(load_ts) AS load_ts, CAST(SUM(bulk) AS DECIMAL(10,5)) / CAST(SUM(item_qty) AS DECIMAL(10,5)) AS bulk, 
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
                WHERE visit_dt >= date_add(current_date(),-1096) 
                GROUP BY store_nbr, visit_nbr, visit_dt''')

        #create temp table for STAGE 1 requirements
        df_sum_values.createOrReplaceTempView("pos_seg_sum_values")

        df_choices_coeff = spark.sql('''SELECT coefficient_id, coefficient FROM gb_customer_data_domain_odl.cdd_odl_pos_seg_choices_config_coefficients''')

        df_choices_coeff.createOrReplaceTempView("coeff_table")

        df_assign_values_coeff = spark.sql('''SELECT ta.store_nbr, ta.visit_nbr, ta.visit_dt, ta.load_ts, tr.triptype_id, mn.low_mission_id,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_c') + 
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_b')) + 
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_ph')) + 
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_pl')) + 
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snobn')) + 
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snotod')) + 
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snoa')) + 
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snoc')) + 
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snocrm')) + 
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snoh')) + 
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snohc')) + 
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snosc')) + 
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snotra')) + 
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ais_snoe')) 
        AS authentic_ingredients_sc, 
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_c') +
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_b')) +
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_ph')) +
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_pl')) +
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snobn')) +
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snotod')) +
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snoa')) +
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snoc')) +
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snocrm')) +
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snoh')) +
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snohc')) +
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snosc')) +
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snotra')) +
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@lhs_snoe'))
        AS
        lowcost_highcal_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_c') +
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_b')) +
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_ph')) +
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_pl')) +
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snobn')) +
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snotod')) +
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snoa')) +
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snoc')) +
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snocrm')) +
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snoh')) +
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snohc')) +
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snosc')) +
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snotra')) +
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@hes_snoe'))
        AS
        healthy_ethical_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_c') +
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_b')) +
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_ph')) +
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_pl')) +
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snobn')) +
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snotod')) +
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snoa')) +
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snoc')) +
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snocrm')) +
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snoh')) +
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snohc')) +
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snosc')) +
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snotra')) +
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@ms_snoe'))
        AS
        mainstream_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_c') +
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_b')) +
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_ph')) +
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_pl')) +
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snobn')) +
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snotod')) +
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snoa')) +
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snoc')) +
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snocrm')) +
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snoh')) +
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snohc')) +
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snosc')) +
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snotra')) +
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@clcs_snoe'))
        AS
        child_led_choices_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_c') +
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_b')) +
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_ph')) +
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_pl')) +
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snobn')) +
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snotod')) +
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snoa')) +
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snoc')) +
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snocrm')) +
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snoh')) +
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snohc')) +
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snosc')) +
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snotra')) +
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@bfbs_snoe'))
        AS
        buying_for_baby_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_c') +
        (ta.bulk * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_b')) +
        (ta.pr_high * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_ph')) +
        (ta.pr_low * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_pl')) +
        (ta.sno_baby_newborn * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snobn')) +
        (ta.sno_toddler * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snotod')) +
        (ta.sno_adventurous * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snoa')) +
        (ta.sno_children * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snoc')) +
        (ta.sno_conv_ready_meals * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snocrm')) +
        (ta.sno_healthy * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snoh')) +
        (ta.sno_high_calorie * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snohc')) +
        (ta.sno_scratch_cooking * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snosc')) +
        (ta.sno_traditional * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snotra')) +
        (ta.sno_ethical * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@sbs_snoe'))
        AS
        stretching_budget_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@nbs_c') +
        (ta.br_own_label * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@nbs_bol')) +
        (ta.br_branded * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@nbs_bb')) AS national_brand_sc,
        (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@obs_c') +
        (ta.br_own_label * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@obs_bol')) +
        (ta.br_branded * (SELECT coefficient FROM coeff_table WHERE coefficient_id = '@obs_bb'))  AS own_brand_sc
        FROM pos_seg_sum_values as ta 
        LEFT JOIN gb_customer_data_domain_odl.cdd_odl_pos_seg_trip as tr ON ta.store_nbr == tr.store_nbr AND ta.visit_nbr == tr.visit_nbr AND ta.visit_dt == tr.visit_dt
        LEFT JOIN gb_customer_data_domain_odl.cdd_odl_pos_seg_mission as mn ON ta.store_nbr == mn.store_nbr AND ta.visit_nbr = mn.visit_nbr AND ta.visit_dt == mn.visit_dt''')

        df_assign_values_coeff.createOrReplaceTempView("values_temp")

        df_pos_seg_choices = spark.sql('''SELECT CAST(CONCAT(REPLACE(CAST(visit_dt AS VARCHAR(10)),'-',''),store_nbr,visit_nbr) AS decimal(38,0)) AS basket_id,
                    store_nbr,
                    visit_nbr,
                    visit_dt,
                    load_ts,
                    triptype_id,
                    low_mission_id,
                    authentic_ingredients_sc,
                    lowcost_highcal_sc,
                    healthy_ethical_sc,
                    mainstream_sc,
                    child_led_choices_sc,
                    buying_for_baby_sc,
                    stretching_budget_sc,
                    national_brand_sc,
                    own_brand_sc,
                    CASE                    
                    WHEN triptype_id = 99 THEN 99 
                    WHEN triptype_id < 5 THEN 98 
                    WHEN 
                    (authentic_ingredients_sc > lowcost_highcal_sc) AND
                    (authentic_ingredients_sc > healthy_ethical_sc) AND
                    (authentic_ingredients_sc > mainstream_sc) AND
                    (authentic_ingredients_sc > child_led_choices_sc) AND
                    (authentic_ingredients_sc > buying_for_baby_sc) AND
                    (authentic_ingredients_sc > stretching_budget_sc) THEN 4
                    
                    WHEN
                    (lowcost_highcal_sc > healthy_ethical_sc) AND
                    (lowcost_highcal_sc > mainstream_sc) AND
                    (lowcost_highcal_sc > child_led_choices_sc) AND
                    (lowcost_highcal_sc > buying_for_baby_sc) AND
                    (lowcost_highcal_sc > stretching_budget_sc) THEN 1
                    
                    WHEN
                    (healthy_ethical_sc > mainstream_sc) AND
                    (healthy_ethical_sc > child_led_choices_sc) AND
                    (healthy_ethical_sc > buying_for_baby_sc) AND
                    (healthy_ethical_sc > stretching_budget_sc) THEN 3
                    
                    WHEN
                    (mainstream_sc > child_led_choices_sc) AND
                    (mainstream_sc > buying_for_baby_sc) AND
                    (mainstream_sc > stretching_budget_sc) AND
                    (own_brand_sc > national_brand_sc) THEN 8
                    
                    WHEN
                    (mainstream_sc > child_led_choices_sc) AND
                    (mainstream_sc > buying_for_baby_sc) AND
                    (mainstream_sc > stretching_budget_sc) AND
                    (national_brand_sc > own_brand_sc) THEN 7
                    
                    WHEN
                    (child_led_choices_sc > buying_for_baby_sc) AND
                    (child_led_choices_sc > stretching_budget_sc) THEN 6
                    
                    WHEN
                    (buying_for_baby_sc > stretching_budget_sc) THEN 5
                    
                    ELSE 2 
                    end AS choices_id
                    
                    FROM values_temp ''')

        # Save df_pos_seg_choices to a new table in Hive to meet physical output requirements
        df_pos_seg_choices.write.partitionBy('visit_dt').mode("overwrite").saveAsTable("gb_customer_data_domain_odl.cdd_odl_pos_seg_choices")

        print("************** SPARK JOB complete****************")

#STAGE 1 = pos_seg_sum_values
#STAGE 4 = values_temp
#STAGE 5 = cdd_odl_pos_seg_choices

ts = pos_seg_choices()
ts.run()
