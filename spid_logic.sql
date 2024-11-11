-- original amendment 14/12/2022
-- filtering table based on join to pc_global_contactable marketing preference.

--select		count(distinct spid.singl_profl_id) as spid_count
--from		(select * from 
--				(select unified_cust_id, singl_profl_id,  atg_customer_id,seg_lifecycle_id,seg_value_id
--				,seg_rfv_id,seg_lifestage_id,seg_pss_id,ttns_dt,postal_sector,channel_grocery_ind,channel_sng_kiosk_ind,channel_sng_mobile_ind
--				,channel_george_ind,channel_baby_ind,channel_loyalty_ind
--				,channel_instore_ind,cdp_extract_ind  
--				from gb_customer_data_domain_rpt.cdd_rpt_customer_spid) cs 
--					INNER JOIN	
--			(select account_status, replace(first_nm, ',', ' ') first_nm, replace(contactable_first_nm, ',', ' ') contactable_first_nm
--			,replace(last_nm, ',', ' ') last_nm, email email,guest_ind,registration_date,registration_channel,gdpr_del_ind,suspend_status
--			,suspend_reason, suspend_ts, tnc_accepted_at_grocery, tnc_accepted_at_sng, tnc_accepted_at_sng_kiosk
--			,tnc_accepted_at_sng_mobile, tnc_accepted_at_george, tnc_accepted_at_btc, tnc_accepted_at_giftcards, tnc_accepted_at_loyalty
--			,last_login_at_grocery,last_login_at_sng_kiosk,last_login_at_sng_mobile, last_login_at_george,last_login_at_asda,last_login_at_btc
--			,last_login_at_giftcards,last_login_at_loyalty,upd_ts,ingest_ts, scndry_login_id scndry_login_id
--			,test_account_ind, singl_profl_id as spid
--			from gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer 
--			where test_account_ind != 'Y'  AND  (last_login_at_grocery >= dateadd(d, -480, getdate()) 
--									OR last_login_at_sng_kiosk >= dateadd(d, -480, getdate()) 
--									OR last_login_at_sng_mobile >= dateadd(d, -480, getdate()) 
--									OR last_login_at_george >= dateadd(d, -480, getdate()) 
--									OR last_login_at_asda >= dateadd(d, -480, getdate()) 
--									OR last_login_at_btc >= dateadd(d, -480, getdate()) 
--									OR last_login_at_giftcards >= dateadd(d, -480, getdate()) 
--									OR last_login_at_loyalty >= dateadd(d, -480, getdate()) )
--				) spc 
--				on cs.singl_profl_id = spc.spid
--			) spid

--inner JOIN	(select single_profile_id as pcg_spid
--				from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]
--				where (last_open_email_date > dateadd(d, -480, getdate()) AND is_email_contactable = 'Y')
--						OR permission_date >= dateadd(d, -60, getdate()) 
--						OR suppression_date >= dateadd(d, -120, getdate()) 
--						OR hard_bounce_date >= dateadd(d, -120, getdate()) 
--						OR on_blacklist = 'Y'  ) pcg 
--on			pcg.pcg_spid = spid.singl_profl_id


-- 2nd logic, pulling back only spids from pc_global_contactable.
--

select			unified_cust_id, singl_profl_id,  atg_customer_id,seg_lifecycle_id,seg_value_id
				,seg_rfv_id,seg_lifestage_id,seg_pss_id,ttns_dt,postal_sector,channel_grocery_ind,channel_sng_kiosk_ind,channel_sng_mobile_ind
				,channel_george_ind,channel_baby_ind,channel_loyalty_ind
				,channel_instore_ind,cdp_extract_ind
				,account_status, replace(first_nm, ',', ' ') first_nm, replace(contactable_first_nm, ',', ' ') contactable_first_nm
				,replace(last_nm, ',', ' ') last_nm, email email,guest_ind,registration_date,registration_channel,gdpr_del_ind,suspend_status
				,suspend_reason, suspend_ts, tnc_accepted_at_grocery, tnc_accepted_at_sng, tnc_accepted_at_sng_kiosk
				,tnc_accepted_at_sng_mobile, tnc_accepted_at_george, tnc_accepted_at_btc, tnc_accepted_at_giftcards, tnc_accepted_at_loyalty
				,last_login_at_grocery,last_login_at_sng_kiosk,last_login_at_sng_mobile, last_login_at_george,last_login_at_asda,last_login_at_btc
				,last_login_at_giftcards,last_login_at_loyalty,upd_ts,ingest_ts, scndry_login_id scndry_login_id
				,test_account_ind
from			(select single_profile_id as pcg_spid
				from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]
				where (last_open_email_date > dateadd(d, -480, getdate()) AND is_email_contactable = 'Y')
						OR permission_date >= dateadd(d, -60, getdate()) 
						OR suppression_date >= dateadd(d, -120, getdate()) 
						OR hard_bounce_date >= dateadd(d, -120, getdate()) 
						OR on_blacklist = 'Y'  ) pcg 
inner JOIN
				(select * from 
						(select unified_cust_id, singl_profl_id,  atg_customer_id,seg_lifecycle_id,seg_value_id
						,seg_rfv_id,seg_lifestage_id,seg_pss_id,ttns_dt,postal_sector,channel_grocery_ind,channel_sng_kiosk_ind,channel_sng_mobile_ind
						,channel_george_ind,channel_baby_ind,channel_loyalty_ind
						,channel_instore_ind,cdp_extract_ind  
						from gb_customer_data_domain_rpt.cdd_rpt_customer_spid) cs 
							INNER JOIN	
						(select account_status, replace(first_nm, ',', ' ') first_nm, replace(contactable_first_nm, ',', ' ') contactable_first_nm
						,replace(last_nm, ',', ' ') last_nm, email email,guest_ind,registration_date,registration_channel,gdpr_del_ind,suspend_status
						,suspend_reason, suspend_ts, tnc_accepted_at_grocery, tnc_accepted_at_sng, tnc_accepted_at_sng_kiosk
						,tnc_accepted_at_sng_mobile, tnc_accepted_at_george, tnc_accepted_at_btc, tnc_accepted_at_giftcards, tnc_accepted_at_loyalty
						,last_login_at_grocery,last_login_at_sng_kiosk,last_login_at_sng_mobile, last_login_at_george,last_login_at_asda,last_login_at_btc
						,last_login_at_giftcards,last_login_at_loyalty,upd_ts,ingest_ts, scndry_login_id scndry_login_id
						,test_account_ind, singl_profl_id as spid
						from gb_customer_data_domain_secured_odl.cdd_odl_singl_profl_customer ) sp 
				
						on cs.singl_profl_id = sp.spid
				) cssp

on			pcg.pcg_spid = cssp.singl_profl_id
;

select max(md_created_ts), max(md_source_ts) from [gb_customer_data_domain_rpt].[cdd_rpt_customer_spid];
select max(md_created_ts), max(md_source_ts) from [gb_customer_data_domain_secured_odl].[cdd_odl_singl_profl_customer];
select max(md_created_ts), max(md_source_ts) from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email];
