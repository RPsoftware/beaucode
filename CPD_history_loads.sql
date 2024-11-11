
/****************
HISTORY LOADS 
******************/
--mssn_wallets
--select count(t.mssn_id), count(distinct t.wallet_id)    
--from [gb_customer_data_domain_odl].[cdd_odl_mssn_wallets] t     
--where rdl_ingest_ts >= dateadd(d, -3, getdate()) ; --3 days = 3,633,442 / 503,923 distinct walletid

--select count(t.mssn_id), count(distinct t.wallet_id)    
--from [gb_customer_data_domain_odl].[cdd_odl_mssn_wallets] t     
--where rdl_ingest_ts >= dateadd(d, -14, getdate()); -- 2 weeks = 36250166 / 5,772,350 distinct walletid

--select count(t.mssn_id), count(distinct t.wallet_id)    
--from [gb_customer_data_domain_odl].[cdd_odl_mssn_wallets] t     
--where rdl_ingest_ts >= dateadd(d, -90, getdate()) ;--90 days/3 months = 406,719,258 / 5,776,361 distinct wallet id

--select count(t.mssn_id), count(distinct t.wallet_id)    
--from [gb_customer_data_domain_odl].[cdd_odl_mssn_wallets] t     
--where rdl_ingest_ts >= dateadd(d, -365, getdate()) ; --1 year = 638,350,124 / 5,778,424 distinct wallet id

--reward wallets 
--select count(reward_id), count( distinct wallet_id)   
--from gb_customer_data_domain_odl.cdd_odl_reward_wallets   
--where rdl_ingest_ts >= dateadd(d, -90, getdate()) ; --90 days = 4,289,323 reward_id / 1719359 distinct wallet_id

--select count(reward_id), count( distinct wallet_id)   
--from gb_customer_data_domain_odl.cdd_odl_reward_wallets   
--where rdl_ingest_ts >= dateadd(d, -365, getdate()) ; -- 365 days = 5198770 reward_id / 1776244 distinct wallet_id

----wallet_pos_txn
-- select count(trans_rcpt_nbr), count(distinct basket_id), count(distinct wallet_id)  
-- from [gb_customer_data_domain_odl].[cdd_odl_wallet_pos_txns] 
-- where event_ts >= dateadd(d, -365, getdate()) ; -- 365 days = 76,758,831 (trans_rcpt_nbr)/ 71067396 (distinct basket_id)/ 4530968 (distinct wallet_id)

---- email_egmt_campaign
-- select count(email), count(distinct singl_profl_id)
-- from gb_customer_data_domain_secured_rpt.cdd_rpt_ft_email_egmt_campaign --full extract 31834578 (email)/28037601(distinct spid)

----email_egmt_customer
--select email, singl_profl_id, unified_cust_id, customer_engagement, engage_rate, communications    
--, first_communication, opens, first_open_journey, last_open, clicks, last_click, unsubscribe, ghs_segment, customer_type
--, is_contactable, contactable_date, ldm_dt_lastload    
--from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_email_egmt_customer] 
--where last_open >= dateadd(d, -360, getdate())   OR last_click > = dateadd(d, -360, getdate())  
--OR contactable_date >= dateadd(d, -360, getdate())

--georgesalesorderlines
--with t1 as (
--select singl_profl_id, unified_cust_id, row_id, sales_order_id, sales_order_line_id, account_number, line_no, item_id, item_type
--, order_quantity,       ordered_full_line_price, ordered_retail_line_price, line_type, bundle_reference, delivery_method
--, collection_store, shipping_postcode, shipping_address_key,       delivery_country, ship_node, scac, order_line_status_id
--, order_line_status, order_line_status_group, return_quantity, refund_value, return_code, return_reason,       ldm_dt_lastload
--, cdd_load_date, creation_date, dih.cons_item_nbr     from gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_order_lines gsol   
--join (select * from (    select a.*, row_number()over(partition by a.item_nbr order by a.item_status_chng_dt)
--row_num,count(1)over(partition by a.item_nbr) cnt            
--from gb_customer_data_domain_odl.cdd_odl_dim_item_hierarchy a)x where row_num = cnt ) dih     
--on dih.item_nbr = gsol.item_id   
--where item_id not like 'G00%'     and singl_profl_id is not null     and gsol.md_created_ts >= dateadd(d, -365, getdate())
--union all  
--select singl_profl_id, unified_cust_id, row_id, sales_order_id, sales_order_line_id, account_number, line_no, item_id, item_type
--, order_quantity,       ordered_full_line_price, ordered_retail_line_price, line_type, bundle_reference, delivery_method
--, collection_store, shipping_postcode, shipping_address_key,       delivery_country, ship_node, scac, order_line_status_id
--, order_line_status, order_line_status_group, return_quantity, refund_value, return_code, return_reason,       ldm_dt_lastload
--, cdd_load_date, creation_date, replace(item_id,'G00','') cons_item_nbr     
--from gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_order_lines gsol   
--where item_id like 'G00%'     and singl_profl_id is not null     and gsol.md_created_ts >= dateadd(d, -365, getdate())
--)

--select count(distinct singl_profl_id) from t1 --6410976 with cons_item_nbr is not null


----george_sales_orders
--select count(1), count( distinct singl_profl_id)
--from gb_customer_data_domain_rpt.cdd_rpt_ft_george_sales_orders gsol       
--where len(singl_profl_id) > 15 and gsol.md_created_ts >= dateadd(d, -365, getdate())
----365 days = 39,993,818 (row count)  / 8,719,312

----groc_del_pass
--select singl_profl_id, unified_cust_id, promo_id, delivery_pass_id, delivery_pass_type, delivery_pass_start_date
--, delivery_pass_end_date    , delivery_pass_cancelled_date, delivery_pass_refund_date, auto_renew, ldm_dt_lastload    
--from gb_customer_data_domain_rpt.cdd_rpt_ft_groc_del_pass    
--where delivery_pass_start_date is not null AND (delivery_pass_start_date >= dateadd(d, -360, getdate())  
--OR delivery_pass_end_date >= dateadd(d, -360, getdate())  
--OR delivery_pass_cancelled_date >= dateadd(d, -360, getdate())   OR delivery_pass_refund_date >= dateadd(d, -360, getdate()))

----grocery_order
--select count(1), count(distinct singl_profl_id) 
--from gb_customer_data_domain_rpt.cdd_rpt_ft_grocery_order
--where cdd_load_date >= dateadd(d, -365, getdate()); 
-- 365 days = 191686323	(row count) / 8216835 (distinct spid)

----rpt_transaction
--select count(t.basket_id), count(distinct ti.singl_profl_id)
--from [gb_customer_data_domain_rpt].[cdd_rpt_transaction] t     
--join [gb_customer_data_domain_odl].[cdd_odl_transaction_ids] ti   
--on t.basket_id = ti.basket_id   
--where t.md_created_ts >= dateadd(d, -365, getdate()) and ti.singl_profl_id is not null 
-- 365 days = 94226230 (row count) / 	6657793 distinct spid 

--transaction_item
--with t1 as (
--select ti.visit_ts, ti.basket_id, ti.cons_item_nbr, ti.original_cin, ti.vendor_nbr      
--,ti.store_nbr,ti.channel_src_id, ti.promotion_ind, till_type, crncy_cd, uom, unmeasured_qty      
--,measured_qty, sale_amt_inc_vat, sale_amt_exc_vat, offer_disc_inc_vat,coll_disc_inc_vat      
--,ti.visit_dt      ,ps.mds_fam_id, ps.asda_promo_type, ps.sales_asda       
--,tids.singl_profl_id, '' as trans_type   
--from   [gb_customer_data_domain_rpt].[cdd_rpt_transaction_item] ti     
--join   (select distinct mds_fam_id, asda_promo_type, sales_asda
--, cons_item_nbr       ,cast(concat(replace(cast(visit_dt as VARCHAR(10)), '-', ''),store_nbr, visit_nbr) as DECIMAL(38,0)) basket_id  
--from [gb_customer_data_domain_odl].[cdd_odl_pos_transaction] where md_created_ts >= dateadd(d, -365, getdate())      ) ps   
--on    ps.basket_id = ti.basket_id  and ti.cons_item_nbr = ps.cons_item_nbr    
--join   [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] ih   
--on    ih.mds_fam_id = ps.mds_fam_id   
--join   [gb_customer_data_domain_odl].[cdd_odl_transaction_ids] tids       
--on ti.basket_id = tids.basket_id  
--where   ti.md_created_ts >= dateadd(d, -365, getdate()) and tids.singl_profl_id IS NOT NULL )
--select count(distinct singl_profl_id), count(1) from t1

----ghs_order_promo
--SELECT count(1) , count(distinct ghs.singl_profl_id)   
--FROM [gb_mb_secured_dl_tables].[ghs_order_kafka] ghs     
--inner join [gb_mb_dl_tables].[ghs_order_promo] promo       
--on ghs.web_order_id = promo.web_order_id       
--where ghs.singl_profl_id is not null  and len(ghs.singl_profl_id) <= 40 
---- 972,137/ 541,460  

----grocery_used_vouchers
--select count(distinct single_profile_id)
--from clbadm.vw_grocery_used_vouchers
--where ldm_dt_lastload >= dateadd(d, -90,getdate()) 
--180 days = 69,866,800 (row count) / 2587499 (distinct spid) / 90 days = 35,995,534 (row count) /1996217 (distinct spid)

--select max(md_created_ts) 
--from [gb_customer_data_domain_secured_raw].[cdd_raw_ft_pc_global_contactable_email]

--select max(md_created_ts)
--from [gb_customer_data_domain_secured_odl].[cdd_odl_singl_profl_customer]

----------------------------
--CUSTOMER SPID TEMP FEED 
----------------------------
  
with t1 as (
select distinct	* 
from			(select single_profile_id as pcg_spid      
				from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]      
				where	(last_open_email_date > dateadd(d, -480, getdate()) AND is_email_contactable = 'Y')       
						OR permission_date >= dateadd(d, -60, getdate())         
						OR suppression_date >= dateadd(d, -120, getdate())        
						OR hard_bounce_date >= dateadd(d, -120, getdate())        
						OR on_blacklist = 'Y'  ) pcg   
				inner join	(select * from       
								(select unified_cust_id, singl_profl_id,  atg_customer_id,seg_lifecycle_id,seg_value_id       
								,seg_rfv_id,seg_lifestage_id,seg_pss_id,ttns_dt,postal_sector,channel_grocery_ind,channel_sng_kiosk_ind
								,channel_sng_mobile_ind        
								,channel_george_ind,channel_baby_ind,channel_loyalty_ind        
								,channel_instore_ind,cdp_extract_ind        
								from gb_customer_data_domain_rpt.cdd_rpt_customer_spid ) cs       
							inner join         
								(select account_status,  
								replace(translate(first_nm,',~#$%\}<{>?*!"','!!!!!!!!!!!!!!'),'!','') first_nm         
								, replace(translate(contactable_first_nm,',~#$%\}<{>?*!"','!!!!!!!!!!!!!!'),'!','') contactable_first_nm         
								, replace(translate(last_nm,',~#$%\}<{>?*!"','!!!!!!!!!!!!!!'),'!','') last_nm
								, email email,guest_ind,registration_date,registration_channel,gdpr_del_ind,suspend_status         
								,suspend_reason, suspend_ts, tnc_accepted_at_grocery, tnc_accepted_at_sng, tnc_accepted_at_sng_kiosk         
								,tnc_accepted_at_sng_mobile, tnc_accepted_at_george, tnc_accepted_at_btc, tnc_accepted_at_giftcards
								, tnc_accepted_at_loyalty         
								,last_login_at_grocery,last_login_at_sng_kiosk,last_login_at_sng_mobile
								, last_login_at_george,last_login_at_asda,last_login_at_btc         
								,last_login_at_giftcards,last_login_at_loyalty,upd_ts,ingest_ts, scndry_login_id scndry_login_id        
								,test_account_ind, singl_profl_id as spid       
								from gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer 
								) sp 
								on cs.singl_profl_id = sp.spid ) cssp
					on pcg.pcg_spid = cssp.singl_profl_id
							  

							union 

		select * from 			(select single_profile_id as pcgmu_spid      
					from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]      
					 ) pcg_mu   
					inner join	(select * from         
									(select unified_cust_id, singl_profl_id,  atg_customer_id,seg_lifecycle_id,seg_value_id       
								,seg_rfv_id,seg_lifestage_id,seg_pss_id,ttns_dt,postal_sector,channel_grocery_ind,channel_sng_kiosk_ind
								,channel_sng_mobile_ind        
								,channel_george_ind,channel_baby_ind,channel_loyalty_ind        
								,channel_instore_ind,cdp_extract_ind        
								from gb_customer_data_domain_rpt.cdd_rpt_customer_spid ) cs_mu       
							inner join         
								(select account_status,  
								replace(translate(first_nm,',~#$%\}<{>?*!"','!!!!!!!!!!!!!!'),'!','') first_nm         
								, replace(translate(contactable_first_nm,',~#$%\}<{>?*!"','!!!!!!!!!!!!!!'),'!','') contactable_first_nm         
								, replace(translate(last_nm,',~#$%\}<{>?*!"','!!!!!!!!!!!!!!'),'!','') last_nm
								, email email,guest_ind,registration_date,registration_channel,gdpr_del_ind,suspend_status         
								,suspend_reason, suspend_ts, tnc_accepted_at_grocery, tnc_accepted_at_sng, tnc_accepted_at_sng_kiosk         
								,tnc_accepted_at_sng_mobile, tnc_accepted_at_george, tnc_accepted_at_btc, tnc_accepted_at_giftcards
								, tnc_accepted_at_loyalty         
								,last_login_at_grocery,last_login_at_sng_kiosk,last_login_at_sng_mobile
								, last_login_at_george,last_login_at_asda,last_login_at_btc         
								,last_login_at_giftcards,last_login_at_loyalty,upd_ts,ingest_ts, scndry_login_id scndry_login_id        
								,test_account_ind, singl_profl_id as spid       
								from gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer 
								where last_login_at_grocery >= dateadd(d, -30, getdate()) 
								OR last_login_at_sng_kiosk >= dateadd(d, -30, getdate()) 
								OR last_login_at_sng_mobile >= dateadd(d, -30, getdate()) 
								OR last_login_at_george >= dateadd(d, -30, getdate()) 
								OR last_login_at_asda >= dateadd(d, -30, getdate()) 
								OR last_login_at_btc >= dateadd(d, -30, getdate()) 
								OR last_login_at_giftcards >= dateadd(d, -30, getdate()) 
								OR last_login_at_loyalty >= dateadd(d, -30, getdate())  
								) sp_mu 
								on cs_mu.singl_profl_id = sp_mu.spid 
							)  cssp_mu
 on pcg_mu.pcgmu_spid = cssp_mu.singl_profl_id

)	

select count(singl_profl_id)
 from t1  --7,492,680, 7,474,628


 -----------------------------
 --transaction item 
 -----------------------------

 select ti.visit_ts, ti.basket_id, ti.cons_item_nbr, ti.original_cin, ti.vendor_nbr      
 ,ti.store_nbr,ti.channel_src_id, ti.promotion_ind, till_type, crncy_cd, uom, unmeasured_qty      
 ,measured_qty, sale_amt_inc_vat, sale_amt_exc_vat, offer_disc_inc_vat,coll_disc_inc_vat      
 ,ti.visit_dt      ,ps.mds_fam_id, ps.asda_promo_type, ps.sales_asda       
 ,tids.singl_profl_id, '' as trans_type   
 from   [gb_customer_data_domain_rpt].[cdd_rpt_transaction_item] ti      
 join   (select distinct mds_fam_id, asda_promo_type, sales_asda, cons_item_nbr       
 ,cast(concat(replace(cast(visit_dt as VARCHAR(10)), '-', ''),store_nbr, visit_nbr) as DECIMAL(38,0)) basket_id             
 from [gb_customer_data_domain_odl].[cdd_odl_pos_transaction] where md_created_ts >= (getdate() - 5)      ) ps  
 on    ps.basket_id = ti.basket_id  and ti.cons_item_nbr = ps.cons_item_nbr    
 join   [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] ih   
 on    ih.mds_fam_id = ps.mds_fam_id   
 join   [gb_customer_data_domain_odl].[cdd_odl_transaction_ids] tids      
 on ti.basket_id = tids.basket_id  
 where   ti.md_created_ts >= (getdate() - 4) and tids.singl_profl_id IS NOT NULL 

 select getdate() - 3 as date 
 select getdate() - 4 as date

 select max(md_created_ts) from [gb_customer_data_domain_rpt].[cdd_rpt_transaction_item]
 select max(md_created_ts) from [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy]
 select max(md_created_ts) from [gb_customer_data_domain_odl].[cdd_odl_transaction_ids]
 select max(md_created_ts) from [gb_customer_data_domain_odl].[cdd_odl_pos_transaction]
 select max(visit_dt) from [gb_customer_data_domain_odl].[cdd_odl_pos_transaction]



 select max(ps.md_created_ts), max(ps.basket_id), max(ps.visit_dt)
 from (select distinct visit_dt, md_created_ts, mds_fam_id, asda_promo_type, sales_asda, cons_item_nbr         
 ,cast(concat(replace(cast(visit_dt as VARCHAR(10)), '-', ''),store_nbr, visit_nbr) as DECIMAL(38,0)) basket_id                
 from [gb_customer_data_domain_odl].[cdd_odl_pos_transaction] where md_created_ts >= (getdate() - 3) )ps

 select max(ti.md_created_ts), max(ti.basket_id)
 from (select * from [gb_customer_data_domain_rpt].[cdd_rpt_transaction_item]  
 where md_created_ts >= (getdate() - 3)  ) ti  
 
 inner join (select distinct md_created_ts, mds_fam_id, asda_promo_type, sales_asda, cons_item_nbr         
 ,cast(concat(replace(cast(visit_dt as VARCHAR(10)), '-', ''),store_nbr, visit_nbr) as DECIMAL(38,0)) basket_id                
 from [gb_customer_data_domain_odl].[cdd_odl_pos_transaction] where md_created_ts >= (getdate() - 3)  ) ps     
 on    ps.basket_id = ti.basket_id  and ti.cons_item_nbr = ps.cons_item_nbr      
 left join   [gb_customer_data_domain_odl].[cdd_odl_dim_item_hierarchy] ih   
 on     ih.mds_fam_id = ps.mds_fam_id   
 left join   [gb_customer_data_domain_odl].[cdd_odl_transaction_ids] tids       
 on ti.basket_id = tids.basket_id  
-- where tids.singl_profl_id IS NOT NULL 