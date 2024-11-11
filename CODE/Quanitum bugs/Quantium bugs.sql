-- transaction_ids / customer basket extract cip flag logic issue 08/2022

-----------------------------------------
 --ROW 2: 14 baskets with cip_rptg_ind = 1
 ----------------------------------------
/* Quanitum feedback: •	Row 2: These baskets have a valid LOYALTY_ID (wallet_id)  but null CUSTOMER_ID (unified_cust_id) and TENDER_CARD_ID (lead xref) so they should be flagged as ‘0’ instead of ‘1’ and 
 * grouped together with the baskets in the first row

**cip_rptg_ind derivation logic**:
	CASE WHEN  (channel_id = '1' AND lead_xref IS NULL) 
		    OR (channel_id = '2' AND singl_profl_id IS NULL)
		  ##  OR (channel_id = '3' AND singl_profl_id IS NULL) ## new logic to include 
   	     OR (channel_id = 2 AND singl_profl_id IS NULL AND lead_xref IS NULL) THEN 0
	ELSE 1 END AS cip_rptg_ind
 */

--looking at examples given in transaction_ids UAT table
select *
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 
WHERE basket_id IN ('202203034676206201919', '202204194386210903100', '202206035869215402301') --examples given in email 

--these are flagged as cip flag = 1 as they fall into the ELSE 1 bucket on that derivation (channel_id = 3 / scan and go) 

-- all 14 baskets:
select basket_id
from gb_customer_data_domain_rpt.cdd_rpt_redesign_customer_basket_uat_2022_08_04 
where cip_rptg_ind = 1 AND unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NULL 

select * 
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_08_05
WHERE basket_id IN (
'202205284934214800851')
'202205244699214403623',
'202205214440214100853',
'202205234692214303495',
'202205234639214304655',
'202205234613214302698',
'202205234274214302249',
'202205224160214202983',
'202205285889214802647',
'202205284657214802090',
'202205315865215102230',
'202206054229215601331',
'202205204576214002708',
'202205305807215000657')

--confirmed, these baskets are all channel_id = 3 so flagged as cip_rptg_ind = 1 


------------------------------------------
-- ROW 3: 67 baskets with cip_rptg_ind = 1
------------------------------------------
/* Quantium feedback: •	Row 3: These baskets have a valid LOYALTY_ID (wallet_id) and TENDER_CARD_ID (leadxref) but null CUSTOMER_ID (unified_cust_id). 
   What are some example customer-types that would fall into this bucket? Our understanding was that if there was a valid tender card ID attached to the basket then we should also have a valid UCID (i.e., customer ID should not be null)
*/

--get all 67 examples 

select basket_id
from gb_customer_data_domain_rpt.cdd_rpt_redesign_customer_basket_uat_2022_08_26
where cip_rptg_ind = 1 AND unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NOT NULL 

SELECT * 
FROM gb_customer_data_domain_secured_raw.cdd_raw_unified_customer_table
WHERE source_system_id in ('5efb0184-287c-479e-ae9c-00409f5d3e07', 'FOMaPVNLCckQ0IrrD1fLqN8uOuAQc3f81ssNlu6eznjLufnlMcWV0/VtZRnvX5nz',
'JFJYeERyFu1iIVIiCdlnth4ppec/7Ysfx584hk5t0mHqho3alxpp1bvk51XN4zir', 'NrE1Is+Xy2jQECYS1wrc4JE+VTNlIvzHFIlrfOUANbaLmS5lgy3WkQOJ1v8Jik7n',  
'XVawhO3l9FLGyIuosa921QIRo3M3SermeXc3D8LTtNcZ2ZAlvDTxOyLaiZ+XPIsv', 'bd2c88d3-3148-4651-9e4e-6b0cfa609e6a', 'cu3YeVgtFNkSqPGa9TT8/ZFO9KKeuvmHwruRASJJwh7m3o5ubqOCeNXMtDVr0Pjm', 
'e1f8aff9-fcfd-4a7e-b7b4-b3efb97d6717', 'e9ef8c34-5261-4e1a-9548-c53bdc4a3535', 'ikRgb6qtXPBdK0CHkmglHyywoepwLl9OLkUdKKYHGFCMcOVL+vJeCQCyoGoelTth', '6e49c971e1eb4d27b0c823d0c1dac12d')


SELECT * 
FROM gb_customer_data_domain_odl.cdd_odl_unified_cust_id_mapping 
WHERE source_system_id in ('5efb0184-287c-479e-ae9c-00409f5d3e07', 'FOMaPVNLCckQ0IrrD1fLqN8uOuAQc3f81ssNlu6eznjLufnlMcWV0/VtZRnvX5nz',
'JFJYeERyFu1iIVIiCdlnth4ppec/7Ysfx584hk5t0mHqho3alxpp1bvk51XN4zir', 'NrE1Is+Xy2jQECYS1wrc4JE+VTNlIvzHFIlrfOUANbaLmS5lgy3WkQOJ1v8Jik7n',  
'XVawhO3l9FLGyIuosa921QIRo3M3SermeXc3D8LTtNcZ2ZAlvDTxOyLaiZ+XPIsv', 'bd2c88d3-3148-4651-9e4e-6b0cfa609e6a', 'cu3YeVgtFNkSqPGa9TT8/ZFO9KKeuvmHwruRASJJwh7m3o5ubqOCeNXMtDVr0Pjm', 
'e1f8aff9-fcfd-4a7e-b7b4-b3efb97d6717', 'e9ef8c34-5261-4e1a-9548-c53bdc4a3535', 'ikRgb6qtXPBdK0CHkmglHyywoepwLl9OLkUdKKYHGFCMcOVL+vJeCQCyoGoelTth', '6e49c971e1eb4d27b0c823d0c1dac12d')

SELECT * 
FROM gb_customer_data_domain_odl.cdd_odl_loyalty_acct 
WHERE singl_profl_id in ('5efb0184-287c-479e-ae9c-00409f5d3e07', 'FOMaPVNLCckQ0IrrD1fLqN8uOuAQc3f81ssNlu6eznjLufnlMcWV0/VtZRnvX5nz',
'JFJYeERyFu1iIVIiCdlnth4ppec/7Ysfx584hk5t0mHqho3alxpp1bvk51XN4zir', 'NrE1Is+Xy2jQECYS1wrc4JE+VTNlIvzHFIlrfOUANbaLmS5lgy3WkQOJ1v8Jik7n',  
'XVawhO3l9FLGyIuosa921QIRo3M3SermeXc3D8LTtNcZ2ZAlvDTxOyLaiZ+XPIsv', 'bd2c88d3-3148-4651-9e4e-6b0cfa609e6a', 'cu3YeVgtFNkSqPGa9TT8/ZFO9KKeuvmHwruRASJJwh7m3o5ubqOCeNXMtDVr0Pjm', 
'e1f8aff9-fcfd-4a7e-b7b4-b3efb97d6717', 'e9ef8c34-5261-4e1a-9548-c53bdc4a3535', 'ikRgb6qtXPBdK0CHkmglHyywoepwLl9OLkUdKKYHGFCMcOVL+vJeCQCyoGoelTth', '6e49c971e1eb4d27b0c823d0c1dac12d')

SELECT * 
FROM gb_customer_data_domain_odl.cdd_odl_loyalty_acct 
WHERE wallet_id in (
'82251073',
'82647538',


select *
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 
WHERE basket_id IN ('202202074676203804634',
'202203034676206201919',
'202205034947212300843',
'202202124576204305975',
'202204054947209500661',
'202204054947209500655',
'202204054274209503265',
'202201294576202905780',
'202201134676201301628',
'202201064676200601696',
'202201104576201005156',
'202203294274208804611',
'202204255869211504111',
'202203254676208403257',
'202203234274208204123',
'202201024576200203043',
'202206044664215504666',
'202206044274215501352',
'202202174676204801540',
'202203104676206901619',
'202204134676210304110',
'202205135869213301982',
'202201204676202001158',
'202202264576205706282',
'202203304947208904515',
'202204124947210200587',
'202204124947210200581',
'202204124386210202782',
'202203195865207803547',
'202205024676212206015',
'202204194947210900590',
'202204194386210903100',
'202205244947214400591',
'202205244947214400586',
'202205244274214403426',
'202205304964215003321',
'202205234274214303631',
'202201314676203103836',
'202203174676207601799',
'202204044676209404850',
'202204264947211600733',
'202205124676213202336',
'202201274676202706741',
'202205104947213000655',
'202205104676213004662',
'202204144386210403905',
'202202064576203703368',
'202203314676209004742',
'202204024964209205027',
'202206014676215204214',
'202206014964215205650',
'202206074947215800648',
'202206074947215800646',
'202205205869214004713',
'202206035869215402301',
'202206064676215704920',
'202206064274215704173',
'202205214989214102414',
'202204185869210804139',
'202204204948211001887',
'202205174947213700692',
'202205174947213700703',
'202205224639214203875',
'202206025869215304402',
'202206024274215300127',
'202205314947215100311',
'202205314947215100301')

-- ALL baskets are channel_id = 3 / flagged as 1 as per current logic 


-- amended current logic, new checks below

select * 
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_04 
WHERE basket_id IN (
'202205284934214800851', -- channel_id = 2 / flag is '1' / singl_profl_id is not null but lead xref IS null 0 pulling spid from loyalty_acct / no ucid in unified_cust_id_mapping for 'sp'
'202205244699214403623',
'202205214440214100853',
'202205234692214303495',
'202205234639214304655',
'202205234613214302698',
'202205234274214302249',
'202205224160214202983',
'202205285889214802647',
'202205284657214802090',
'202205315865215102230',
'202206054229215601331',
'202205204576214002708',
'202205305807215000657')

select * from gb_customer_data_domain_odl.cdd_odl_unified_cust_id_mapping where `source` = 'sp' and source_system_id = '6e49c971e1eb4d27b0c823d0c1dac12d' -- no results 
select * from gb_customer_data_domain_odl.cdd_odl_ucid_mapping_latest where source_system_id = '6e49c971e1eb4d27b0c823d0c1dac12d' -- no results
select * from gb_customer_data_domain_odl.cdd_odl_ucid_mapping_latest where source_system_id = '6e49c971-e1eb-4d27-b0c8-23d0c1dac12d' -- trying different format
select * from gb_customer_data_domain_odl.cdd_odl_unified_cust_id_mapping where source_system_id = '6e49c971e1eb4d27b0c823d0c1dac12d'

select * from gb_customer_data_domain_odl.cdd_odl_ucid_mapping_latest where source_system_id = 'e1f8aff9-fcfd-4a7e-b7b4-b3efb97d6717' OR source_system_id = '126341529710'
select * from gb_customer_data_domain_odl.cdd_odl_ucid_mapping_latest where source_system_id = 'NrE1Is+Xy2jQECYS1wrc4JE+VTNlIvzHFIlrfOUANbaLmS5lgy3WkQOJ1v8Jik7n' OR source_system_id = '119874183872'
select * from gb_customer_data_domain_odl.cdd_odl_ucid_mapping_latest where source_system_id = 'e9ef8c34-5261-4e1a-9548-c53bdc4a3535' OR source_system_id = '95036986917'

 -- e1f8aff9-fcfd-4a7e-b7b4-b3efb97d6717
 -- 6e49c971-e1eb-4d27-b0c8-23d0c1dac12d



select *
from gb_customer_data_domain_rpt.cdd_rpt_redesign_customer_basket_uat_2022_08_04 
where cip_rptg_ind = 1 AND unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NULL 

select basket_id, wallet_id, singl_profl_id, lead_xref, channel_id
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_08_05
WHERE basket_id = '202201054364200500996'


----
--what quantium are querying against + counts from latest rpt layer table
----
--row 1 check 
select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26
where unified_cust_id IS NULL  AND wallet_id IS NOT NULL AND lead_xref IS NULL
group by cip_rptg_ind

--total should be 264,103

--row 2 / row 7 check
select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26
where unified_cust_id IS NOT NULL AND wallet_id IS NULL and lead_xref IS NOT NULL 
group by cip_rptg_ind

--total should be 249,900,989

--row 3/ row 9 check 
select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26
where unified_cust_id IS NOT NULL AND wallet_id IS NOT NULL AND lead_xref IS NOT NULL 
group by cip_rptg_ind

--total should be 1,440,789

--row 6
select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26
where unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NOT NULL 
group by cip_rptg_ind

-- totals should be 19,878,150

-- row 8 
select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26
where unified_cust_id IS NOT NULL AND wallet_id IS NOT NULL AND lead_xref IS NULL 
group by cip_rptg_ind

--totals should be 19,683


-- NEW BUG --
select *
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26
where unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NOT NULL and cip_rptg_ind = 1


				Select * from gb_customer_data_domain_rpt.cdd_rpt_customer_basket cb
				inner join gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 ti on cb.basket_id = ti.basket_id 
				inner join gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_26 rcb on rcb.basket_id = ti.basket_id 
WHERE cip_rptg_ind = '0'
----------------------------------------------------------------------------------------------------------------------



select *
from gb_customer_data_domain_rpt.cdd_rpt_redesign_customer_basket_uat_2022_08_04 
where cip_rptg_ind = 1 AND unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NOT NULL 


select * 
from gb_customer_data_domain_rpt.cdd_rpt_redesign_customer_basket_uat_2022_08_04 
where cip_rptg_ind = 0 AND unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NULL 

with data1 as (
select *
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12 
WHERE basket_id IN ('202202074676203804634',
'202203034676206201919',
'202205034947212300843',
'202202124576204305975',
'202204054947209500661',
'202204054947209500655',
'202204054274209503265',
'202201294576202905780',
'202201134676201301628',
'202201064676200601696',
'202201104576201005156',
'202203294274208804611',
'202204255869211504111',
'202203254676208403257',
'202203234274208204123',
'202201024576200203043',
'202206044664215504666',
'202206044274215501352',
'202202174676204801540',
'202203104676206901619',
'202204134676210304110',
'202205135869213301982',
'202201204676202001158',
'202202264576205706282',
'202203304947208904515',
'202204124947210200587',
'202204124947210200581',
'202204124386210202782',
'202203195865207803547',
'202205024676212206015',
'202204194947210900590',
'202204194386210903100',
'202205244947214400591',
'202205244947214400586',
'202205244274214403426',
'202205304964215003321',
'202205234274214303631',
'202201314676203103836',
'202203174676207601799',
'202204044676209404850',
'202204264947211600733',
'202205124676213202336',
'202201274676202706741',
'202205104947213000655',
'202205104676213004662',
'202204144386210403905',
'202202064576203703368',
'202203314676209004742',
'202204024964209205027',
'202206014676215204214',
'202206014964215205650',
'202206074947215800648',
'202206074947215800646',
'202205205869214004713',
'202206035869215402301',
'202206064676215704920',
'202206064274215704173',
'202205214989214102414',
'202204185869210804139',
'202204204948211001887',
'202205174947213700692',
'202205174947213700703',
'202205224639214203875',
'202206025869215304402',
'202206024274215300127',
'202205314947215100311',
'202205314947215100301')

SELECT distinct * FROM data1 LEFT JOIN (select `source`, customerid, source_system_id, version from gb_customer_data_domain_secured_raw.cdd_raw_unified_customer_table) uc on data1.lead_xref = uc.source_system_id 
LEFT JOIN (select `source`, customerid, source_system_id, version from gb_customer_data_domain_secured_raw.cdd_raw_unified_customer_table) uc2 on data1.singl_profl_id = uc2.source_system_id 

--get distinct wallet _ids and check if the exist in wallet_pos_txns with the same spid - does anything look odd here?
select * 
gb_customer_data_domain_secured_raw.cdd_raw_unified_customer_table
From gb_customer_data_domain_raw.cdd_raw_wallet_pos_txns 
where wallet_id in (
'82767885',
'87017268',
'82251073',
'89425532',
'85971900',
'89937602',
'82647538',
'87129756',
'87981666',
'82193403')



---DREMIO QUERIES
select * 
From CustomerDataDomain.RAW."loyalty_acct"
where singl_profl_id in ('XVawhO3l9FLGyIuosa921QIRo3M3SermeXc3D8LTtNcZ2ZAlvDTxOyLaiZ+XPIsv', 
'cu3YeVgtFNkSqPGa9TT8/ZFO9KKeuvmHwruRASJJwh7m3o5ubqOCeNXMtDVr0Pjm', 
'NrE1Is+Xy2jQECYS1wrc4JE+VTNlIvzHFIlrfOUANbaLmS5lgy3WkQOJ1v8Jik7n',
'j++SKObxuPMRXKdxyMxkPySXxGY39/XoQLRRrSLUMAI8rwsyGKXkTa0LdyiHwzLQ',
'5/a6jivhsbg6CgwJkNRN/mq0UnZ9Lz3epNwBhwIQbNOJzqoWrg73/+hhv85raXnM',
'JFJYeERyFu1iIVIiCdlnth4ppec/7Ysfx584hk5t0mHqho3alxpp1bvk51XN4zir',
'FOMaPVNLCckQ0IrrD1fLqN8uOuAQc3f81ssNlu6eznjLufnlMcWV0/VtZRnvX5nz', 
'ikRgb6qtXPBdK0CHkmglHyywoepwLl9OLkUdKKYHGFCMcOVL+vJeCQCyoGoelTth',
'kh6PBztfqjo9/kUhgvuLPc4bKr847SvAZAKm3A0wDQ85oA/7G6IGJCJbL+0iEKPF')
--'e1f8aff9-fcfd-4a7e-b7b4-b3efb97d6717',

/*select * 
From CustomerDataDomain.RAW."wallet_pos_txns"
where wallet_id in (
'82767885',
'87017268',
'82251073',
'89425532',
'85971900',
'89937602',
'82647538',
'87129756',
'87981666',
'82193403')
*/



---- latest bugs 

-- row 2 vs row 7 
select cip_rptg_ind, count(cb.basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_04 cb left join gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_08_05 ti on cb.basket_id = ti.basket_id
where cb.wallet_id IS NULL AND cb.unified_cust_id IS NOT NULL AND cb.lead_xref IS NOT NULL and ti.non_scan_visit_ind = 0 and ti.channel_id = 3 and ti.singl_profl_id is NULL 
group by cip_rptg_ind 


select cip_rptg_ind, channel_id, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
where unified_cust_id IS NOT NULL AND wallet_id IS NULL and lead_xref IS NOT NULL 
group by cip_rptg_ind, channel_id
-- follow up 
	select basket_id 
	from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
	where unified_cust_id IS NOT NULL AND wallet_id IS NULL and lead_xref IS NOT NULL AND cip_rptg_ind = 0
	LIMIT 10
	
	select * 
	FROM gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_08_05
	WHERE basket_id IN ('202203154786207402279', '202203154645207403659', '202203154157207402190') -- results 16

	select basket_id 
	from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
	where unified_cust_id IS NOT NULL AND wallet_id IS NULL and lead_xref IS NOT NULL AND cip_rptg_ind = 1
	LIMIT 10
	
	select * 
	FROM gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_08_05
	WHERE basket_id IN ('202201034641200302122', '202201034980200303821', '202201034183200302785') -- results 18
	
	
	

select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
where unified_cust_id IS NOT NULL AND wallet_id IS NOT NULL and lead_xref IS NOT NULL 
group by cip_rptg_ind

-- 
select count(basket_id), count(singl_profl_id), count(wallet_id), count(lead_xref)
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_08_05

-- 
select count(basket_id), count(singl_profl_id), count(wallet_id), count(lead_xref)
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 


select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
where unified_cust_id IS NOT NULL AND wallet_id IS NULL and lead_xref IS NOT NULL 
group by cip_rptg_ind


select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
where unified_cust_id IS NOT NULL AND wallet_id IS NOT NULL AND lead_xref IS NOT NULL 
group by cip_rptg_ind


select cip_rptg_ind, count(basket_id)
from gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12
where unified_cust_id IS NULL AND wallet_id IS NOT NULL AND lead_xref IS NULL 
group by cip_rptg_ind


---checking lengths of spid--
SELECT * 
FROM gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 
WHERE length(singl_profl_id) = 32
LIMIT 10


select length(singl_profl_id), count(*)
from gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 
group by length(singl_profl_id) 

----
--new table 
-----

select *
FROM gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_09_16 ncb
inner join gb_customer_data_domain_odl.cdd_odl_redesign_customer_basket_uat_2022_08_12 ocb on ncb.basket_id = ocb.basket_id
 WHERE ncb.basket_id 
 IN ('202206025869215304402',
'202204255869211504111',
'202205135869213301982',
'202206035869215402301',
'202204204948211001887',
'202204185869210804139',
'202205205869214004713' )


--looking at customer_basket vs transaction-ids
SELECT * 
FROM gb_customer_data_domain_odl.cdd_odl_transaction_ids_uat_2022_05_27 a INNER JOIN 
 gb_customer_data_domain_odl.cdd_odl_customer_basket b ON a.basket_id = b.basket_id INNER JOIN 
gb_customer_data_domain_raw.cdd_raw_sng_visit c on a.tc_nbr = c.trans_nbr       
WHERE a.visit_dt < '2022-06-09' and b.basket_id NOT IN (SELECT basket_id FROM gb_customer_data_domain_rpt.cdd_rpt_redesign_customer_basket_uat_2022_08_26) and 
 a.basket_id = '202206044948215500456'

