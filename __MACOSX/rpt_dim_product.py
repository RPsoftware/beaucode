from cip.cip.framework.connections.spark import spark
from cip.cip.framework.files.read import cnt


class rptProduct:

    def __init__(self):
        """
        Assigning Variable from config file
        """
        self.target_db = cnt["Customer_Staging_Mart"]["cdd_rpt_database"]["extraction_database"]
        self.target_table = 'cdd_rpt_qpromo_product'

    def run(self):
        print("*******************Execution Start*********************")
        sql_text_scan = '''SELECT aa.original_cin,aa.mds_fam_id,aa.from_sales_dt,aa.to_sales_dt,aa.vatsales,aa.salesrank
                            FROM
                            (
                             SELECT cs.original_cin,cs.mds_fam_id,cm.from_sales_dt,cm.to_sales_dt,SUM(cs.vatsales) AS vatsales
                                    ,row_number() OVER (PARTITION BY cs.original_cin ORDER BY SUM(cs.vatsales) DESC, cs.mds_fam_id DESC) AS salesrank
                                FROM
                                 (SELECT
                                     CASE WHEN m.mds_fam_id IS NOT NULL AND s.scan_type = 0 THEN m.original_cin WHEN i.item_nbr IS NOT NULL AND s.scan_type = 4 THEN i.original_cin ELSE NULL END AS original_cin
                                    ,CASE WHEN m.mds_fam_id IS NOT NULL AND s.scan_type = 0 THEN m.mds_fam_id WHEN i.item_nbr IS NOT NULL AND s.scan_type = 4 THEN i.mds_fam_id ELSE NULL END AS mds_fam_id
                                    ,s.visit_dt,SUM(s.vat_sell_price_amt) AS vatsales
                                    FROM gb_customer_data_domain_raw.cdd_raw_store_visit_scan s
                                    LEFT JOIN gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy m ON s.scan_id = m.mds_fam_id
                                    LEFT JOIN gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy i ON s.scan_id = i.item_nbr
                                    GROUP BY
                                    CASE WHEN m.mds_fam_id IS NOT NULL AND s.scan_type = 0 THEN m.original_cin WHEN i.item_nbr IS NOT NULL AND s.scan_type = 4 THEN i.original_cin ELSE NULL END
                                   ,CASE WHEN m.mds_fam_id IS NOT NULL AND s.scan_type = 0 THEN m.mds_fam_id WHEN i.item_nbr IS NOT NULL AND s.scan_type = 4 THEN i.mds_fam_id ELSE NULL END
                                    ,s.visit_dt
                                ) cs
                                INNER JOIN
                                (
                                SELECT
                                 CASE WHEN m.mds_fam_id IS NOT NULL AND s.scan_type = 0 THEN m.original_cin WHEN i.item_nbr IS NOT NULL AND s.scan_type = 4 THEN i.original_cin ELSE NULL END AS original_cin
                                ,DATE_SUB(MAX(visit_dt), 29) AS from_sales_dt
                                ,MAX(visit_dt) AS to_sales_dt
                                 FROM gb_customer_data_domain_raw.cdd_raw_store_visit_scan s
                                 LEFT JOIN gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy m ON s.scan_id = m.mds_fam_id
                                 LEFT JOIN gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy i ON s.scan_id = i.item_nbr
                                 GROUP BY
                                 CASE WHEN m.mds_fam_id IS NOT NULL AND s.scan_type = 0 THEN m.original_cin WHEN i.item_nbr IS NOT NULL AND s.scan_type = 4 THEN i.original_cin ELSE NULL END
                                 ) cm
                            ON cs.original_cin = cm.original_cin AND cs.visit_dt BETWEEN cm.from_sales_dt AND cm.to_sales_dt
                            GROUP BY cs.original_cin,cs.mds_fam_id,cm.from_sales_dt,cm.to_sales_dt
                            ) aa
                            WHERE aa.salesrank = 1 '''

        sql_text_item_hierarchy = '''SELECT cons_item_nbr, original_cin, mds_fam_id,item_nbr
                                          ,CASE WHEN concat_prod_desc_gp is not NULL then concat_prod_desc_gp ELSE concat_item_desc END as PRODUCT_NAME
                                          ,concat_item_desc
                                          ,concat_prod_desc_gp
                                          ,item_desc_1
                                          ,color_desc
                                          ,size_desc
                                          ,signing_desc
                                          ,prod_disp_nm
                                          ,PRIMARY_EAN
                                          ,HIERARCHY_LEVEL_1_ID
                                          ,HIERARCHY_LEVEL_1_NAME
                                          ,HIERARCHY_LEVEL_2_ID
                                          ,HIERARCHY_LEVEL_2_NAME
                                          ,HIERARCHY_LEVEL_3_ID
                                          ,HIERARCHY_LEVEL_3_NAME
                                          ,HIERARCHY_LEVEL_4_ID
                                          ,HIERARCHY_LEVEL_4_NAME
                                          ,MDSE_SUBCATG_ID
                                          ,MDSE_SUBCATG_NAME
                                          ,EFF_DATE_TIME
                                          ,VENDOR_ID
                                          ,OWN_LABEL_CD
                                          ,BRAND_ID
                                          ,brand
                                          ,SUB_BRAND
                                          ,SUPER_BRAND
                                          ,ASSORTMENT_ID
                                          ,ASSORTMENT
                                          ,SUB_VARIANT
                                          ,product_variant
                                          ,product_sub_variant
                                          ,COUNTRY
                                          ,DIET
                                          ,PACK_SIZE
                                          ,CASE WHEN a.pack_size > 1 THEN 'PK' ELSE 'EA' END AS PACK_SIZE_UOM
                                          ,CAST(CASE WHEN a.pack_size = 1 AND a.sell_uom_cd = 'EA' THEN 1 WHEN COALESCE(a.pack_size,0) = 0 THEN 1 ELSE a.sell_qty/a.pack_size END AS DECIMAL(18,3)) as SINGLE_SIZE
                                          ,sell_uom_cd as SINGLE_SIZE_UOM
                                          ,sell_qty as TOTAL_SIZE
                                          ,sell_qty_gp
                                          ,PACK_SIZE_WITH_UNITS
                                          ,SINGLE_SIZE_WITH_UNITS
                                          ,TOTAL_SIZE_WITH_UNITS
                                          ,TOTAL_SIZE_KGL
                                          ,ALCOHOL_VOLUME
                                          ,ALCOHOL_BAND
                                          ,PRICE_BANDS
                                          ,PACK_CONFIGURATION
                                          ,PACK_TYPE
                                          ,REGION
                                          ,BASE_TYPE
                                          ,FLAVOUR_VARIETY
                                          ,BRAND_PACK_SIZE
                                          ,HFSS
                                          ,Organic
                                          ,FreeFrom
                                          ,Vegan
                                          ,Vegetarian
                                          ,World_Food
                                          ,Local
                                          ,Colour
                                          ,Article_Nbr
                                          ,Product_Family_1
                                          ,Product_Family_2
                                          FROM
                                          (SELECT cons_item_nbr, h.original_cin, mds_fam_id,item_nbr,picker_desc
                                                 ,CASE WHEN item_desc_1 = signing_desc
                                                    then trim(CONCAT(item_desc_1,' ',coalesce(color_desc,''),' ',coalesce(size_desc,'')))
                                                    ELSE trim(CONCAT(item_desc_1,' ',signing_desc))
                                                    END as concat_item_desc
                                                 ,CASE WHEN prod_disp_nm IS NULL THEN NULL
                                                       WHEN prod_disp_nm is not NULL and prod_size_qty = 'EACH'
                                                       THEN CASE WHEN brand_nm_gp is NULL THEN prod_disp_nm
                                                       ELSE CONCAT(brand_nm_gp,' ',prod_disp_nm) END
                                                       WHEN prod_disp_nm is not NULL and prod_size_qty != 'EACH'
                                                       THEN CASE WHEN brand_nm_gp is NULL THEN CONCAT(prod_disp_nm,' ',coalesce(prod_size_qty,''))
                                                       ELSE CONCAT(brand_nm_gp,' ',prod_disp_nm,' ',coalesce(prod_size_qty,'')) END
                                                       END as concat_prod_desc_gp
                                                ,item_desc_1
                                                ,color_desc
                                                ,size_desc
                                                ,signing_desc
                                                ,prod_disp_nm
                                                ,upc_nbr as PRIMARY_EAN
                                                ,CONCAT(LPAD(dept_nbr,2,'0'),LPAD(fineline_nbr,4,'0000')) as HIERARCHY_LEVEL_1_ID
                                                ,CASE WHEN fineline_desc is not NULL and fineline_desc != ''
                                                    then fineline_desc ELSE CONCAT('PPG - ', CONCAT(LPAD(dept_nbr,2,'0'),LPAD(fineline_nbr,4,'0000'))) END as HIERARCHY_LEVEL_1_NAME
                                                ,CONCAT(LPAD(dept_nbr,2,'0'),LPAD(mdse_catg_nbr,5,'00000')) as HIERARCHY_LEVEL_2_ID
                                                ,mdse_catg_desc as HIERARCHY_LEVEL_2_NAME
                                                ,dept_nbr as HIERARCHY_LEVEL_3_ID
                                                ,dept_desc as HIERARCHY_LEVEL_3_NAME
                                                ,catg_id as HIERARCHY_LEVEL_4_ID
                                                ,catg_desc as HIERARCHY_LEVEL_4_NAME
                                                ,CONCAT(LPAD(dept_nbr,2,'0'),LPAD(mdse_catg_nbr,5,'00000'),LPAD(mdse_subcatg_nbr,2,'0')) as MDSE_SUBCATG_ID
                                                ,mdse_subcatg_desc as MDSE_SUBCATG_NAME
                                                ,item_create_dt as EFF_DATE_TIME
                                                ,vendor_nbr as VENDOR_ID
                                                ,CASE WHEN brand_id != '5' THEN 'Y' ELSE 'N' END as OWN_LABEL_CD
                                                ,CASE WHEN brand_id is null THEN '-999' ELSE brand_id END as BRAND_ID
                                                ,CASE WHEN b.brand_nm is NULL
                                                                    THEN
                                                                        CASE WHEN brand_nm_gp is NULL
                                                                            THEN CASE WHEN h.brand_nm is not null  and h.brand_nm != ''
                                                                                THEN h.brand_nm
                                                                                ELSE 'Unknown' END
                                                                            ELSE brand_nm_gp END
                                                                    ELSE b.brand_nm
                                                                    END AS brand
                                                ,b.sub_brand
                                                ,CASE WHEN b.super_brand is NULL THEN
                                                            CASE WHEN h.product_variant != '' THEN h.product_variant
                                                            END
                                                      ELSE b.super_brand
                                                      END as product_variant
                                                ,CAST (Null as string) as  ASSORTMENT_ID
                                                ,CAST (Null as string) as ASSORTMENT
                                               ,CAST (Null as string) as SUB_VARIANT
                                                ,CASE WHEN b.super_brand is NULL THEN
                                                            CASE WHEN brand_fam_nm != '' THEN brand_fam_nm
                                                            END
                                                      ELSE b.super_brand
                                                      END as SUPER_BRAND
                                                ,h.product_variant as VARIANT
                                                ,b.product_sub_variant
                                                ,CAST (Null as string) as COUNTRY
                                                ,CASE WHEN diet_type_desc != '' THEN diet_type_desc  END as DIET
                                                ,sell_qty
                                                ,prod_size_qty as sell_qty_gp
                                                ,sell_uom_cd
                                                ,CAST (Null as string) as PACK_SIZE_WITH_UNITS
                                                ,CAST (Null as string) as SINGLE_SIZE_WITH_UNITS
                                                ,CAST (Null as string) as TOTAL_SIZE_WITH_UNITS
                                                ,CAST (CASE WHEN sell_uom_cd = 'GR' THEN sell_qty/1000
                                                            WHEN sell_uom_cd = 'ML' THEN sell_qty/1000
                                                            WHEN sell_uom_cd = 'C3' THEN sell_qty/100
                                                            WHEN sell_uom_cd = 'CM' THEN sell_qty/100
                                                            WHEN sell_uom_cd = 'OZ' THEN sell_qty/35.2739907229
                                                            WHEN sell_uom_cd = 'PT' THEN sell_qty/1.75975398639
                                                            ELSE NULL END as decimal(10,5)) as TOTAL_SIZE_KGL
                                                ,alcohol_pct as ALCOHOL_VOLUME
                                                ,CAST (Null as string) as ALCOHOL_BAND
                                                ,CAST (Null as string) as PRICE_BANDS
                                                ,CAST (Null as string) as PACK_CONFIGURATION
                                                ,CAST (Null as string) as PACK_TYPE
                                                ,b.region as REGION
                                                ,CAST (Null as string) as BASE_TYPE
                                                ,CAST (Null as string) as FLAVOUR_VARIETY
                                                ,CAST (Null as string) as BRAND_PACK_SIZE
                                                ,CAST(CASE WHEN LOCATE('X', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA'
                                                           THEN sell_qty
                                                           WHEN LOCATE('X', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('X', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('X', UPPER(size_desc))-1))
                                                           WHEN LOCATE('*', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA'
                                                           THEN sell_qty
                                                           WHEN LOCATE('*', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('*', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('*', UPPER(size_desc))-1))
                                                           WHEN LOCATE("\'", UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE("\'", UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE("\'", UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE("\'", UPPER(size_desc))-1))
                                                           WHEN LOCATE('PK', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('PK', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('PK', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('PK', UPPER(size_desc))-1))
                                                           WHEN LOCATE('PACK', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('PACK', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('PACK', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('PACK', UPPER(size_desc))-1))
                                                           WHEN LOCATE('PC', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('PC', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('PC', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('PC', UPPER(size_desc))-1))
                                                           WHEN LOCATE('ROLL', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('ROLL', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('ROLL', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, LOCATE('ROLL', 1, UPPER(size_desc))-1))
                                                           WHEN LOCATE('LENS', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('LENS', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('LENS', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('LENS', UPPER(size_desc))-1))
                                                           WHEN LOCATE('EA', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('EA', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('EA', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('EA', UPPER(size_desc))-1))
                                                           WHEN LOCATE('PAC', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('PAC', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('PAC', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('PAC', UPPER(size_desc))-1))
                                                           WHEN LOCATE('BAGS', UPPER(size_desc)) > 0 AND sell_uom_cd = 'EA' THEN sell_qty
                                                           WHEN LOCATE('BAGS', UPPER(size_desc)) > 1 AND CAST(TRIM(SUBSTR(size_desc, 1, LOCATE('BAGS', UPPER(size_desc))-1)) as double) >= 0
                                                           THEN TRIM(SUBSTR(size_desc, 1, LOCATE('BAGS', UPPER(size_desc))-1))
                                                           WHEN bu_id <> 9 AND sell_uom_cd = 'EA'
                                                           THEN sell_qty
                                                           ELSE 1 END as DECIMAL(18,0)) AS pack_size
                                                ,b.HFSS, b.Organic, b.FreeFrom, b.Vegan, b.Vegetarian, b.World_food, b.Local, b.colour, b.article_nbr, b.product_family_1, b.product_family_2
                                            FROM gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy  h
                                            left outer join gb_customer_data_domain_raw.cdd_raw_qpromo_item_brand_override b on h.original_cin = b.original_cin
                                            ) a '''

        sql_text_prod_attr = ''' SELECT cons_item_nbr, old_nbr, baby_new_born, toddler, adventurous, children, convenience_readymeals, convenience_timesaving, ethical, healthy, high_calorie, low_calorie, scratch_cooking, traditional, vegetarian, own_label, branded, own_label_choice, branded_choice, own_label_smartprice, own_label_extraspecial, own_label_goodforyou, own_label_freefrom, own_label_standard, own_label_asdaother, organic, essentials_staples, essentials_core, essentials_occasional, essentials_niche, essentials_infrequent, essentials_marginal, high_price, mid_price, low_price, bulk, st_foodtogo, ambient_drinks, ambient_food, baby, bakery, bws, confectionery_cakes, dairy_milk_cream, dairy_other, fresh_meals_deli, fresh_mfp, fresh_produce, frozen, health_beauty, home_leisure, household_cleaning, pet_cat, pet_dog, pet_other
                                     FROM gb_customer_data_domain_odl.cdd_odl_product_attributes '''

        sql_result_txt = '''SELECT
                                    A.CONS_ITEM_NBR
                                    ,A.ORIGINAL_CIN
                                    ,A.item_nbr as latest_item_nbr
                                    ,A.PRODUCT_NAME AS PRODUCT_DESC
                                    ,A.concat_item_desc
                                    ,A.concat_prod_desc_gp
                                    ,A.item_desc_1
                                    ,A.color_desc
                                    ,A.size_desc
                                    ,A.signing_desc
                                    ,A.prod_disp_nm
                                    ,A.PRIMARY_EAN AS UPC_NBR
                                    ,A.HIERARCHY_LEVEL_1_ID AS FINELINE_NBR
                                    ,A.HIERARCHY_LEVEL_1_NAME AS FINELINE_DESC
                                    ,A.HIERARCHY_LEVEL_2_ID AS MDSE_CATG_NBR
                                    ,A.HIERARCHY_LEVEL_2_NAME AS MDSE_CATG_DESC
                                    ,A.HIERARCHY_LEVEL_3_ID AS DEPT_NBR
                                    ,A.HIERARCHY_LEVEL_3_NAME AS DEPT_DESC
                                    ,A.HIERARCHY_LEVEL_4_ID AS CATG_NBR
                                    ,A.HIERARCHY_LEVEL_4_NAME AS CATG_DESC
                                    ,A.MDSE_SUBCATG_ID AS MDSE_SUBCATG_NBR
                                    ,A.MDSE_SUBCATG_NAME AS MDSE_SUBCATG_DESC
                                    ,A.EFF_DATE_TIME AS ITEM_CREATE_DT
                                    ,A.VENDOR_ID AS VENDOR_NBR
                                    ,v.vendor_nm as vendor_nm
                                    ,A.OWN_LABEL_CD AS OWN_LABEL_CD
                                    ,A.BRAND_ID AS BRAND_ID
                                    ,A.BRAND AS BRAND_NM
                                    ,A.SUB_BRAND AS SUB_BRAND
                                    ,A.SUPER_BRAND AS SUPER_BRAND
                                    ,A.ASSORTMENT_ID AS ASSORTMENT_ID
                                    ,A.ASSORTMENT AS ASSORTMENT
                                    ,A.SUB_VARIANT AS SUB_VARIANT
                                    ,A.PRODUCT_VARIANT
                                    ,A.PRODUCT_SUB_VARIANT
                                    ,A.COUNTRY AS COUNTRY
                                    ,A.DIET AS DIET_TYPE_DESC
                                    ,A.PACK_SIZE AS SELL_PKG_QTY
                                    ,A.PACK_SIZE_UOM AS PACK_SIZE_UOM
                                    ,A.SINGLE_SIZE AS SINGLE_SIZE
                                    ,A.SINGLE_SIZE_UOM AS SINGLE_SIZE_UOM
                                    ,A.TOTAL_SIZE AS SELL_QTY
                                    ,A.sell_qty_gp
                                    ,A.SINGLE_SIZE_UOM AS SELL_UOM_CD
                                    ,A.PACK_SIZE_WITH_UNITS AS PACK_SIZE_WITH_UNITS
                                    ,A.SINGLE_SIZE_WITH_UNITS AS SINGLE_SIZE_WITH_UNITS
                                    ,A.TOTAL_SIZE_WITH_UNITS AS TOTAL_SIZE_WITH_UNITS
                                    ,A.TOTAL_SIZE_KGL AS TOTAL_SIZE_KGL
                                    ,A.ALCOHOL_VOLUME AS ALCOHOL_PCT
                                    ,A.ALCOHOL_BAND AS ALCOHOL_BAND
                                    ,A.PRICE_BANDS AS PRICE_BANDS
                                    ,A.PACK_CONFIGURATION AS PACK_CONFIGURATION
                                    ,A.PACK_TYPE AS PACK_TYPE
                                    ,A.REGION AS REGION
                                    ,A.BASE_TYPE AS BASE_TYPE
                                    ,A.FLAVOUR_VARIETY AS FLAVOUR_VARIETY
                                    ,A.BRAND_PACK_SIZE AS BRAND_PACK_SIZE
                                    ,baby_new_born,toddler,adventurous,children,convenience_readymeals,convenience_timesaving,ethical,healthy,high_calorie,low_calorie,scratch_cooking
                                    ,traditional,vegetarian,own_label,branded,own_label_choice,branded_choice,own_label_smartprice,own_label_extraspecial,own_label_goodforyou,own_label_freefrom,own_label_standard,own_label_asdaother,organic,essentials_staples,essentials_core,essentials_occasional,essentials_niche
                                   ,essentials_infrequent,essentials_marginal,high_price,mid_price,low_price,bulk,st_foodtogo,ambient_drinks,ambient_food,baby,bakery,bws,confectionery_cakes,dairy_milk_cream,dairy_other,fresh_meals_deli,fresh_mfp,fresh_produce,frozen,health_beauty,home_leisure,household_cleaning
                                    ,pet_cat,pet_dog,pet_other
                                    ,CASE WHEN f.hierarchy_level_id is null then A.HIERARCHY_LEVEL_1_NAME
                                     else f.new_name end FINELINE_DESC_CIP
                                    ,CASE WHEN m.hierarchy_level_id is null then A.HIERARCHY_LEVEL_2_NAME
                                     else m.new_name end MDSE_CATG_DESC_CIP
                                    ,CASE WHEN d.hierarchy_level_id is null then A.HIERARCHY_LEVEL_3_NAME
                                     else d.new_name end DEPT_DESC_CIP
                                     ,HFSS, Organic, FreeFrom, Vegan, Vegetarian, World_food,local, colour, article_nbr, product_family_1, product_family_2
                                  FROM ITEM_HIERARCHY A INNER JOIN SCAN_DATA B ON A.MDS_FAM_ID = B.MDS_FAM_ID
                                  LEFT JOIN gb_customer_data_domain_rpt.cdd_rpt_vendor v ON A.VENDOR_ID = v.VENDOR_NBR
                                  LEFT JOIN gb_customer_data_domain_raw.cdd_raw_item_hierarchy_rename f on
                                  f.hierarchy_level_name = 'fineline_nbr' and A.HIERARCHY_LEVEL_1_ID = f.hierarchy_level_id
                                  LEFT JOIN gb_customer_data_domain_raw.cdd_raw_item_hierarchy_rename m on
                                  m.hierarchy_level_name = 'mdse_catg_nbr' and A.HIERARCHY_LEVEL_2_ID = m.hierarchy_level_id
                                  LEFT JOIN gb_customer_data_domain_raw.cdd_raw_item_hierarchy_rename d on
                                  d.hierarchy_level_name = 'dept_nbr' and A.HIERARCHY_LEVEL_3_ID = d.hierarchy_level_id
                                  LEFT JOIN prod_attr pa ON A.original_cin = pa.cons_item_nbr and A.item_nbr = pa.old_nbr '''

        scan_data_df = spark.sql(sql_text_scan)
        scan_data_df = scan_data_df.filter(scan_data_df.salesrank == '1')
        scan_data_df.createOrReplaceTempView("scan_data")

        item_hierarchy_df = spark.sql(sql_text_item_hierarchy) #here is where item_brand_override attributes are added
        item_hierarchy_df.createOrReplaceTempView("item_hierarchy")

        prod_attr_df = spark.sql(sql_text_prod_attr)
        prod_attr_df.createOrReplaceTempView('prod_attr')

        result_df = spark.sql(sql_result_txt)
	
	result_df1 = result_df.dropDuplicates()

        result_df1.write.mode("overwrite").saveAsTable("{}.{}".format(self.target_db, self.target_table))
        print("*******************Execution End*********************")


aa = rptProduct()
aa.run()
