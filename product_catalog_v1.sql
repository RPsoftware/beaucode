--george table 

with		george_cat as (
select		cast(item_cd as varchar) as skuID,
			cast(dih.cons_item_nbr as varchar)  as product_code_CIN,
			dih.cons_item_desc as product_desc,
			item_extd_desc as product_desc2,
			(select cast (dih.catg_id as varchar(40)) +' '+'-'+ ' ' + dih.catg_desc) as product_type,
			'' as product_link,
			'' as Image_link,
			'Yes' as Online_Availability
from		[gb_mb_oms_dl_tables].[george_item] gi
inner join	[gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih
on			dih.item_desc_1 = gi.Item_short_desc
and			item_status_desc = 'ACTIVE'
and			trad_divn <> 'FOOD'
and			item_cd not like '000%' )
select		cast(dih.item_nbr as varchar)  as skuID,
			cast(dih.cons_item_nbr as varchar)  as product_code_CIN,
			dih.cons_item_desc as product_desc,
			'' as product_desc2,
			(select cast (dih.catg_id as varchar(40)) +' '+'-'+ ' ' + dih.catg_desc) as product_type,
			'' as product_link,
			'' as Image_link,
			'Yes' as Online_Availability
from		[gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih
where		item_status_desc = 'ACTIVE'
and			trad_divn not in ('PETROL', 'MOBILE')
and			cons_item_nbr not in (select product_code_CIN from george_cat)
union 
select		* 
from		george_cat