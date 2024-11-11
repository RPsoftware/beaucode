/*
checking suppression rules counts 
*/
select count(*), max(event_ts), min(event_ts) from [gb_customer_data_domain_odl].[cdd_odl_wallet_pos_txns]
 select trans_rcpt_nbr, chnl_nm, trans_nbr, visit_nbr
 , store_nbr, reg_nbr, tc_nbr, basket_id, wallet_id, src_create_ts, event_ts, cdd_upd_ts, rdl_ingest_ts, sale_amt_inc_vat, visit_dt
 from [gb_customer_data_domain_odl].[cdd_odl_wallet_pos_txns] where event_ts >= dateadd(d, -7, getdate())

 select top 50 *
	from [gb_wm_vm].[stock_keeping_unit]

	select *
from [gb_wm_vm].[stock_keeping_unit] 
where on_hand_qty !='0' and on_hand_qty is not null 



-- pc_permission_changes 
select upd_ts
from [gb_customer_data_domain_odl].[cdd_odl_pc_permission_changes]
where upd_ts >= dateadd(d, -360, getdate())

--customer_spid (data from odl_singl_prof_customer)
select count(1)  
from [gb_customer_data_domain_secured_odl].[cdd_odl_singl_profl_customer]
where last_login_at_grocery >= dateadd(d, -360, getdate())
	OR last_login_at_sng_kiosk >= dateadd(d, -360, getdate())
	OR last_login_at_sng_mobile >= dateadd(d, -360, getdate())
	OR last_login_at_george >= dateadd(d, -360, getdate())
	OR last_login_at_asda >= dateadd(d, -360, getdate())
	OR last_login_at_btc >= dateadd(d, -360, getdate())
	OR last_login_at_giftcards >= dateadd(d, -360, getdate())
	OR last_login_at_loyalty >= dateadd(d, -360, getdate())

with test2 as(
select cs.[unified_cust_id] unified_cust_id
      ,cs.[singl_profl_id] singl_profl_id
      ,cs.[atg_customer_id] atg_customer_id
      ,seg_lifecycle_id
      ,seg_value_id
      ,seg_rfv_id
      ,seg_lifestage_id
      ,seg_pss_id
      ,ttns_dt
      ,postal_sector
      ,channel_grocery_ind
      ,channel_sng_kiosk_ind
      ,channel_sng_mobile_ind
      ,channel_george_ind
      ,channel_baby_ind
      ,channel_loyalty_ind
      ,channel_instore_ind
      ,cdp_extract_ind 
      ,spc.[account_status] account_status
      , replace(first_nm, ',', ' ') first_nm
      , replace(contactable_first_nm, ',', ' ') contactable_first_nm
      , replace(last_nm, ',', ' ') last_nm
      , email email
      ,guest_ind
      ,registration_date
      ,registration_channel
      ,gdpr_del_ind
      ,suspend_status
      ,suspend_reason
      ,suspend_ts      
      ,tnc_accepted_at_grocery
      ,tnc_accepted_at_sng
      ,tnc_accepted_at_sng_kiosk
      ,tnc_accepted_at_sng_mobile
      ,tnc_accepted_at_george
      ,tnc_accepted_at_btc
      ,tnc_accepted_at_giftcards
	  ,tnc_accepted_at_loyalty
      ,last_login_at_grocery
      ,last_login_at_sng_kiosk
      ,last_login_at_sng_mobile
      ,last_login_at_george
      ,last_login_at_asda
      ,last_login_at_btc
      ,last_login_at_giftcards
      ,last_login_at_loyalty
      ,upd_ts
      ,ingest_ts
	  , scndry_login_id scndry_login_id
 from gb_customer_data_domain_rpt.cdd_rpt_customer_spid cs 
	INNER JOIN gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer spc on cs.singl_profl_id = spc.singl_profl_id
 where test_account_ind != 'Y'  AND
 (last_login_at_grocery >= dateadd(d, -360, getdate()) 
	OR last_login_at_sng_kiosk >= dateadd(d, -360, getdate()) 
	OR last_login_at_sng_mobile >= dateadd(d, -360, getdate()) 
	OR last_login_at_george >= dateadd(d, -360, getdate()) 
	OR last_login_at_asda >= dateadd(d, -360, getdate()) 
	OR last_login_at_btc >= dateadd(d, -360, getdate()) 
	OR last_login_at_giftcards >= dateadd(d, -360, getdate()) 
	OR last_login_at_loyalty >= dateadd(d, -360, getdate()) 
  )  )

  select count(*) from test2 where 
				email like '%,%' or
				first_nm like '%,%' or
				contactable_first_nm like '%,%' or
				last_nm like '%,%'

--email_egmt_customer
select  count(1) 
from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_email_egmt_customer]
where last_open >= dateadd(d, -360, getdate())
 OR last_click > = dateadd(d, -360, getdate())
	OR contactable_date >= dateadd(d, -360, getdate())

-- george_activity
select count(*) 
from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_george_activity]
where last_order_date >= dateadd(d, -360, getdate())

select email_id, singl_profl_id, unified_cust_id
		  ,george_registration_date, guest_customer, number_of_orders
		  ,avg_order_val_1y, orders_1y, orders_isc_1y, orders_del_1y
		  ,last_order_date,store_id_last_isc, ldm_dt_lastload
		  from gb_customer_data_domain_secured_rpt.cdd_rpt_ft_george_activity where convert(datetime, last_order_date) >= dateadd(d, -360, getdate()) 
			and email_id IS NOT NULL

select min(last_order_date)
		  from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_george_activity]
where last_order_date >= dateadd(dd, -360, getdate())
--group by last_order_date

--groc_del_pass
select count(*) 
from [gb_customer_data_domain_rpt].[cdd_rpt_ft_groc_del_pass]
where delivery_pass_start_date >= dateadd(d, -360, getdate()) 
	OR delivery_pass_end_date >= dateadd(d, -360, getdate())
	OR delivery_pass_cancelled_date >= dateadd(d, -360, getdate())
	OR delivery_pass_refund_date >= dateadd(d, -360, getdate())

-- grocery_activity
select count(1) 
from [gb_customer_data_domain_rpt].[cdd_rpt_ft_grocery_activity]
where last_login_at_grocery >= dateadd(d, -360, getdate())
	OR last_order_date >= dateadd(d, -360, getdate())



--pc_global_contactable_email
select top 1500 *
from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]
where (last_open_email_date > dateadd(d, -300, getdate()) 
	OR permission_date >= dateadd(d, -30, getdate()) 
	OR suppression_date >= dateadd(d, -30, getdate()) ) AND contact_first_name IS NOT NULL


	with test1 as (select single_profile_id, hashbytes('SHA2_256',(replace(contact_first_name, ',',' '))) contact_first_name, hashbytes('SHA2_256',lower(email)) email, is_email_invalid
  , is_string_excluded, on_blacklist, hard_bounce, hard_bounce_date, is_suspended, is_valid_contact, permission_value
  , permission_date, suppression_value, suppression_date, is_email_contactable, international_customer, last_open_email_date, gdpr_engaged, global_preference_flag, snapshot_date 
 	from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]
	where cast(last_open_email_date as date) > dateadd(d, -30, getdate()) 
	OR cast(permission_date as date) >= dateadd(d, -30, getdate()) 
	OR cast(suppression_date as date) >= dateadd(d, -30, getdate()) )

select count(*)
	from test1 

--scan_go_activity
select count(1) 
from [gb_customer_data_domain_rpt].[cdd_rpt_ft_scan_go_activity]
where last_login_at_sng_kiosk >= dateadd(d, -360, getdate()) 
	OR last_login_at_sng_mobile >= dateadd(d, -360, getdate())
	OR last_completed_transaction_date >= dateadd(d, -360, getdate())

--loyalty acct 
select md_source_ts, count(1) 
from [gb_customer_data_domain_secured_rpt].[cdd_rpt_loyalty_acct]
where permission_date >=dateadd(d,-360,getdate()) 
	OR last_login_ts >= dateadd(d, -360, getdate())
	OR last_loyalty_scan_ts >=dateadd(d,-360, getdate())
group by md_source_ts