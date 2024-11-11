/****** Script for SelectTopNRows command from SSMS  ******/
--SELECT * FROM [metadata].[ConfigFileExtractor];

update [metadata].[ConfigFileExtractor]
set sourcesqlquery = 'select cast(item_cd as varchar) as skuID,
			cast(dih.cons_item_nbr as varchar)  as product_code_CIN,
			replace(translate(dih.cons_item_desc, ''~#$%\}<>{?*!"'',''!!!!!!!!!!!!!''),''!'','''') as CIN_desc,
			case when picker_desc is null then replace(translate(item_extd_desc, ''~#$%\}<>{?*!"'',''!!!!!!!!!!!!!''),''!'','''')
			else picker_desc end as product_desc,
			replace(translate(item_extd_desc, ''~#$%\}<>{?*!"'',''!!!!!!!!!!!!!''),''!'','''') as product_desc2,
			(select cast (dih.catg_id as varchar(40)) +'' ''+''-''+ '' '' + dih.catg_desc) as product_type,
			'''' as product_link,
			'''' as Image_link,
			''Yes'' as Online_Availability
from		[gb_mb_oms_dl_tables].[george_item] gi
inner join	[gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih
on			dih.item_desc_1 = gi.Item_short_desc
and			item_status_desc = ''ACTIVE''
and			trad_divn <> ''FOOD''
and			item_cd not like ''000%'' 

union 

select		cast(dih.item_nbr as varchar)  as skuID,
			cast(dih.cons_item_nbr as varchar)  as product_code_CIN,
			replace(translate(dih.cons_item_desc,''~#$%\}<>{?*!"'',''!!!!!!!!!!!!!''),''!'','''') as CIN_desc,
			picker_desc as product_desc,
			'''' as product_desc2,
			(select cast (dih.catg_id as varchar(40)) +'' ''+''-''+ '' '' + dih.catg_desc) as product_type,
			'''' as product_link,
			'''' as Image_link,
			''Yes'' as Online_Availability
from		[gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih
where		item_status_desc = ''ACTIVE''
and			trad_divn not in (''PETROL'', ''MOBILE'')
and			cons_item_nbr not in 
				(select cast(dih.cons_item_nbr as varchar)  as product_code_CIN
				from		[gb_mb_oms_dl_tables].[george_item] gi
				inner join	[gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih
				on			dih.item_desc_1 = gi.Item_short_desc
				and			item_status_desc = ''ACTIVE''
				and			trad_divn <> ''FOOD''
				and			item_cd not like ''000%'' ) '
where id = 57;