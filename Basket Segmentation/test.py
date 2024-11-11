class fundingTable:
    def __init__(self):

        self.rpt_db = 'gb_customer_data_domain_rpt'
        self.odl_db = 'gb_customer_data_domain_odl'
        self.raw_db = 'gb_customer_data_domain_raw'

        self.item = 'cdd_odl_dim_item_hierarchy'

        self.target_table = 'cdd_rpt_qpromo_funding'

    def run(self):
        print('********START*****************')

        ams_micoe = spark.sql('''select * from gb_customer_data_domain_raw.cdd_raw_ams_micoe_gl_data_gb ''')
        ams_micoe.createOrReplaceTempView('ams')
        print(ams_micoe.count())
        volume = spark.sql('''select distinct ip_agreement as agreement_number, ams_department as department_num, legacy_vendor_number as legacy_vendor_number \
                                        , allowance_typ as allowance_type, allwtyp_desc as allowance_type_description \
                                        from gb_customer_data_domain_raw.cdd_raw_ams_micoe_volume_agreement_gb where ams_department <> '0' ''')
        volume.createOrReplaceTempView('vol')
        print(volume.count())
        fixed = spark.sql('''select distinct agreement_number,department_num as department_num,legacy_vendor_number as legacy_vendor_number,allowance_type,allowance_type_description
                                        from gb_customer_data_domain_raw.cdd_raw_ams_micoe_fixed_agreement_gb ''')
        fixed.createOrReplaceTempView('fix')
        print(fixed.count())

        dih = spark.sql('''select mds_fam_id, original_cin, dept_nbr, item_nbr from gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy where original_cin IS NOT NULL''')
        dih.createOrReplaceTempView('dih')

        final_df = spark.sql(''' select \
                                dih.original_cin \
                                ,posting_date \
                                ,ams.agreement_number \
                                ,agreement_type \
                                ,department \
                                ,department_description \
                                ,ams.mdse_number \
                                ,mdse_description \
                                ,reason_code \
                                ,b.allowance_type_description \
                                ,b.legacy_vendor_number \
                                ,SUM(cancellation_amount) as cancellation_amount \
                                ,SUM(allocation_amt_lc) as funding_amount \
                                ,SUM(allocation_amt_dc) as allocation_amt_dc \
                                FROM ams \
                                left join (vol union fix) b \
                                on ams.department = b.department_num and ams.agreement_number = b.agreement_number \
                                left join dih on dih.item_nbr = ams.Item_number \
                                AND dih.dept_nbr = ams.department \
                                where (agreement_type = 'ZAMV' and original_cin IS NOT NULL) OR (agreement_type = 'ZAMF' and original_cin IS NULL) \
                                group by dih.original_cin, posting_date, ams.agreement_number, agreement_type, department, department_description, ams.mdse_number, mdse_description \
                                ,reason_code, b.allowance_type_description, b.legacy_vendor_number ''')

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        final_df.printSchema()