------------------------------------------------------------------------------------------
--OFFERS 
------------------------------------------------------------------------------------------

--count * 	
--	select * from gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers LIMIT 10
-- 313,467

--Should be no Rollback promotions
--SELECT PROMO_ID, PRODUCT_ID, Asda_Promo_Type, PRICING_STORE_FORMAT,count(*) as counting
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE PRODUCT_ID != '4681141' AND ASDA_PROMO_TYPE = 'Rollback'
--GROUP BY PROMO_ID, PRODUCT_ID, Asda_Promo_Type, PRICING_STORE_FORMAT
--having count(*) > 1
--order by counting desc

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers 
--WHERE MIN_COLLECTION_DATE > MAX_COLLECTION_DATE
--;

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers 
--WHERE Asda_Promo_Type IN ('Linksave and Rollback') and PROMO_ID_LS is null

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE Asda_Promo_Type = 'Rollback' and PROMO_ID_RB is null
--
--SELECT PRICING_STORE_FORMAT ,Asda_Promo_Type, COUNT(*) as NULLS
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE PROMO_START_DATE is null or PROMO_END_DATE is NULL
--GROUP BY PRICING_STORE_FORMAT, Asda_Promo_Type
--;

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE PRICING_STORE_FORMAT = 'Convenience' and PROMO_START_DATE is null
---- Can these be ignored as there are no Rollbacks in Convenience?


--SELECT DISTINCT PRICING_STORE_FORMAT 
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--;

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE Asda_Promo_Type IN ('Linksave', 'Linksave and Rollback')
--AND (LINKSAVE_START_DATE is null or LINKSAVE_END_DATE is null or PROMO_START_DATE is null or PROMO_END_DATE is null)
--;
--
--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE Asda_Promo_Type IN ('Linksave', 'Linksave and Rollback')
--	AND (LINKSAVE_START_DATE > LINKSAVE_END_DATE) 
--;
--
--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE Asda_Promo_Type IN ('Linksave', 'Linksave and Rollback')
--	AND (PROMO_START_DATE > PROMO_END_DATE) 
--;

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE Asda_Promo_Type IN ('Linksave', 'Linksave and Rollback')
--	AND Asda_Linksave_Desc is null
--;

--SELECT *
--FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
--WHERE Asda_Promo_Type IN ('Linksave', 'Linksave and Rollback')
--	AND (LINKSAVE_QTY_TRIGGER is null OR LINKSAVE_TOTAL_PRICE is null or LINKSAVE_UNIT_PRICE is null)
--;


--SELECT * FROM gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
-- WHERE PROMO_ID = '305871LCV' and PRODUCT_ID = 2043462 order by MIN_COLLECTION_DATE
------ Highlighting that Squashing algorithm working correctly

select gregorian_date, ldm_dt_lastload, count(*) 
from gb_customer_data_domain_raw.cdd_raw_qpromo_sku_ty_dly_mumd_unpivot
group by gregorian_date, ldm_dt_lastload

--cogs catchup gb_customer_data_domain_odl.cdd_odl_cogs_catchup
select sale_date, count(*)
from gb_customer_data_domain_odl.cdd_odl_cogs_catchup
group by sale_date order by sale_date desc

select sale_date, count(*) 
from gb_customer_data_domain_rpt.cdd_rpt_cogs_catchup
group by sale_date order by sale_date

select collection_date, loaded_date, count(*)
from gb_customer_data_domain_raw.cdd_raw_fct_pricing_promo_daily
group by collection_date, loaded_date order by loaded_date desc

select max_collection_date, count(*)
from gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
group by max_collection_date order by max_collection_date desc 

select *
from gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
where max_collection_date = '2023-11-24' and product_id = '1331'


select product_id, count(*) 
from gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
where max_collection_date = '2023-11-24'
group by product_id HAVING count(*) > 1

select product_id, asda_promo_type, count(*) 
from gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
where max_collection_date = '2023-11-24'
group by product_id, asda_promo_type HAVING count(*) > 1

select asda_promo_type, count(*) 
from gb_customer_data_domain_rpt.cdd_rpt_qpromo_offers
where max_collection_date = '2023-11-24'
group by asda_promo_type 

