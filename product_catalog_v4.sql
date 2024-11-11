/*product catalog 
		Need to amend to get unique skuid 
		
*/

--current query --
select cast(item_cd as varchar) as SkuID,
case when item_cd like 'G00%' then replace(cast(item_cd as varchar), 'G00', '')
else cast(dih.cons_item_nbr as varchar)
end as ProductCode, --product code is either george item_cd or cons_item_nbr 
case when picker_desc is null then replace(replace(translate(gi.item_extd_desc, '~#$%\}<>{?*!"','!!!!!!!!!!!!!'),'!',''), 'Do NOT SUB','')
else replace(replace(translate(picker_desc, '~#$%\}<>{?*!"','!!!!!!!!!!!!!'),'!',''), 'DO NOT SUB', '')
end as ProductName,
(select cast (dih.catg_id as varchar(40)) +' '+'-'+ ' ' + dih.catg_desc) as ProductType,
'' as ProductLink,
'' as ImageLink,
'00.00' as RegularPrice,
'Yes' as OnlineAvailability
from [gb_mb_oms_dl_tables].[george_item] gi
inner join [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih on dih.item_desc_1 = gi.Item_short_desc 
and item_status_desc = 'ACTIVE' 
and trad_divn <> 'FOOD' 
and item_cd not like '000%'

--proposed query -- 
select cast(item_cd as varchar) as SkuID,
case when item_cd like 'G00%' then replace(cast(item_cd as varchar), 'G00', '')
else cast(dih.cons_item_nbr as varchar) 
end as ProductCode, 
prod.product_desc as ProductName,
(select cast (dih.catg_id as varchar(40)) +' '+'-'+ ' ' + dih.catg_desc) as ProductType,
'' as ProductLink,
'' as ImageLink,
'00.00' as RegularPrice,
'Yes' as OnlineAvailability
from [gb_mb_oms_dl_tables].[george_item] gi
inner join [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih on dih.item_desc_1 = gi.Item_short_desc
left join [gb_customer_data_domain_rpt].[cdd_rpt_product] prod on dih.cons_item_nbr = prod.cons_item_nbr 
where item_cd not like '000%' and item_cd not like '00%' and item_cd not like 'GEM%'
and item_status_desc = 'ACTIVE' and trad_divn <> 'FOOD' and item_cd not like '000%' and prod.product_desc is not null 

union

select cast(dih.item_nbr as varchar) as SkuID, cast(dih.cons_item_nbr as varchar) as ProductCode, prod.product_desc as ProductName, (select cast (dih.catg_id as varchar(40)) +' '+'-'+ ' ' + dih.catg_desc) as ProductType, '' as ProductLink,  '' as ImageLink,  '00.00' as RegularPrice,     
            'Yes' as OnlineAvailability  
from        [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih  
left join    (select product_desc, latest_item_nbr 
                from [gb_customer_data_domain_rpt].[cdd_rpt_product]) prod  
on            prod.latest_item_nbr = dih.cons_item_nbr  
where        item_status_desc = 'ACTIVE'  
and            trad_divn not in ('PETROL', 'MOBILE')
and            prod.product_desc is not null 
and            cast(item_nbr as varchar) not in       
                (select cast(item_cd as varchar)     
                    from  [gb_mb_oms_dl_tables].[george_item] gi      
                    inner join [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] dih      
                    on   dih.item_desc_1 = gi.Item_short_desc      
                    and   item_status_desc = 'ACTIVE'      
                    and   trad_divn <> 'FOOD'      
                    and item_cd not like '000%' 
					and item_cd not like '00%' 
					and item_cd not like 'GEM%'
					)