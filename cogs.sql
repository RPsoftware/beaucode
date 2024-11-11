select 
		cog.mds_fam_id
		,cog.original_cin
		,cog.store_nbr
		,cog.sale_date
		,cog.unit_cost_price
		,cog.sale_retail_cost_amount
		,cog.units_sold_cem
		,cog.sale_amt_exc_vat_cem
		,ti.sale_amt_exc_vat_trans
		,ti.units_sold_trans
		,item_qty as in_the_bin_qty
		,(item_qty * cog.unit_cost_price) as total_cost_in_the_bin
from (
			select Distinct original_cin
			,mds_fam_id
			,store_nbr
			,sale_date
			,unit_cost_price
			,units_sold as units_sold_cem
			,sale_retail_cost_amount
			,sales_amount_excluding_vat as sale_amt_exc_vat_cem
			from "UKProd1_Hive"."gb_customer_data_domain_odl"."cdd_odl_cogs" group by original_cin
			,mds_fam_id
			,store_nbr
			,sale_date
			, unit_cost_price
			,units_sold
			,sale_retail_cost_amount
			,units_sold_cem
			,sales_amount_excluding_vat )cog
		full outer join (Select distinct item_qty, item_nbr, store_nbr, gregorian_date from UKProd1_Hive.gb_customer_data_domain_raw.cdd_raw_qpromo_sku_ty_dly_mumd_unpivot where event_id in (5600, 1600, 1814, 5814))mumd
		on cog.mds_fam_id = mumd.item_nbr
		and mumd.store_nbr = cog.store_nbr
		and mumd.gregorian_date = cog.sale_date
		left join (select distinct
		original_cin
		,visit_dt
		,store_nbr
		,sum (sale_amt_exc_vat) as sale_amt_exc_vat_trans
		,sum(unmeasured_qty) as units_sold_trans
from "UKProd1_Hive"."gb_customer_data_domain_rpt".cdd_rpt_transaction_item group by original_cin
		,visit_dt
		,store_nbr ) ti
		on cog.original_cin = ti.original_cin
		and sale_date = visit_dt
		and cog.store_nbr = ti.store_nbr
		and ti.units_sold_trans > 0
		where cog.original_cin = '8601'
		and cog.store_nbr = '4229'
		and cog.sale_date = '2023-03-22'