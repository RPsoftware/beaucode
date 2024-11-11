-- ===========================================================================
-- Create database template for Azure Synapse SQL Analytics on-demand Database
-- ===========================================================================

--CREATE DATABASE quantium
--GO 


--CREATE EXTERNAL DATA SOURCE [businessEnrich] WITH (LOCATION = N'abfss://business@saaslenhdtadmstaguks01.dfs.core.windows.net')
--GO


--CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] WITH (FORMAT_TYPE = PARQUET)
--GO


--CREATE SCHEMA qpromo
--GO 

--CREATE USER [sg-rg-data-nonprod-business-internal-read] FROM  EXTERNAL PROVIDER 
--GO

----------------------------------------------------------------------------------------------------------------------------------------------------------------------

/****** Object:  Table [qpromo].[cdd_raw_cust_activity_cip]    Script Date: 20/02/2024 20:55:32 ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[qpromo].[cdd_raw_cust_activity_cip]') AND type in (N'U'))
DROP EXTERNAL TABLE [qpromo].[cdd_raw_cust_activity_cip]
GO

/****** Object:  Table [qpromo].[cdd_raw_cust_activity_cip]    Script Date: 20/02/2024 20:55:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

CREATE EXTERNAL TABLE [qpromo].[cdd_raw_cust_activity_cip]
(
	[unified_cust_id] [varchar](20),
	[is_active_cip] [varchar](1),
	[md_process_id] [varchar](200),
	[md_source_ts] [datetime2](7),
	[md_created_ts] [datetime2](7),
	[md_source_path] [varchar](200)
)
WITH (DATA_SOURCE = [businessEnrich],LOCATION = N'/internal/quantium/quantium/qpromo/cdd_raw_cust_activity_cip/',FILE_FORMAT = [SynapseParquetFormat])
GO

GRANT SELECT ON Object::[quantium].[qpromo].[cdd_raw_cust_activity_cip] TO [sg-rg-data-nonprod-business-internal-read];
----------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE [qpromo].[cdd_raw_quantium_item_brand_override]
(
	[product_name] [varchar](255),
	[article_nbr] [varchar](255),
	[original_cin] [int],
	[category_id] [int],
	[category_name] [varchar](255),
	[department_id] [int],
	[department_name] [varchar](255),
	[merchandising_category_id] [int],
	[merchandising_category_name] [varchar](255),
	[merchandising_subcategory_id] [int],
	[merchandising_subcategory_name] [varchar](255),
	[product_profile_group_id] [int],
	[product_profile_group_name] [varchar](255),
	[super_brand] [varchar](255),
	[brand_nm] [varchar](255),
	[sub_brand] [varchar](255),
	[product_variant] [varchar](255),
	[product_sub_variant] [varchar](255),
	[vendor_id] [int],
	[vendor_name] [varchar](255),
	[grandparent_vendor_id] [int],
	[grandparent_vendor_name] [varchar](255),
	[hfss] [varchar](255),
	[organic] [varchar](255),
	[freefrom] [varchar](255),
	[vegan] [varchar](255),
	[vegetarian] [varchar](255),
	[world_food] [varchar](255),
	[local] [varchar](255),
	[country_of_origin] [varchar](255),
	[flavour] [varchar](255),
	[colour] [varchar](255),
	[single_size] [int],
	[single_size_uom] [varchar](255),
	[single_size_with_units] [varchar](255),
	[pack_size] [varchar](255),
	[pack_size_uom] [varchar](255),
	[pack_size_with_units] [varchar](255),
	[total_size] [int],
	[total_size_uom] [varchar](255),
	[total_size_with_units] [varchar](255),
	[alcohol_band] [varchar](255),
	[alcohol_volume] [varchar](255),
	[base_type] [varchar](255),
	[diet] [varchar](255),
	[pack_configuration] [varchar](255),
	[pack_type] [varchar](255),
	[price_bands] [varchar](255),
	[region] [varchar](255),
	[cdt_1] [varchar](255),
	[cdt_2] [varchar](255),
	[cdt_3] [varchar](255),
	[cdt_4] [varchar](255),
	[cdt_5] [varchar](255),
	[cdt_6] [varchar](255),
	[cdt_7] [varchar](255),
	[cdt_8] [varchar](255),
	[cdt_9] [varchar](255),
	[cdt_10] [varchar](255),
	[cdt_11] [varchar](255),
	[cdt_12] [varchar](255),
	[cdt_13] [varchar](255),
	[cdt_14] [varchar](255),
	[cdt_15] [varchar](255),
	[md_process_id] [varchar](200),
	[md_source_ts] [datetime2](7),
	[md_created_ts] [datetime2](7),
	[md_source_path] [varchar](200)
)
WITH (DATA_SOURCE = [businessEnrich],LOCATION = N'/internal/quantium/quantium/qpromo/cdd_raw_quantium_item_brand_override/',FILE_FORMAT = [SynapseParquetFormat])
GO

GRANT SELECT ON Object::[quantium].[qpromo].[cdd_raw_quantium_item_brand_override] TO [sg-rg-data-nonprod-business-internal-read];

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE [qpromo].[cdd_raw_cip_segments]
(
	[unified_cust_id] [varchar](200),
	[seg_value_id] [decimal](38, 0),
	[segment_id] [varchar](200),
	[eff_from_dt] [date],
	[eff_to_dt] [date],
	[md_process_id] [varchar](200),
	[md_source_ts] [datetime2](7),
	[md_created_ts] [datetime2](7),
	[md_source_path] [varchar](200)
)
WITH (DATA_SOURCE = [businessEnrich],LOCATION = N'/internal/quantium/quantium/qpromo/cdd_raw_cip_segments/',FILE_FORMAT = [SynapseParquetFormat])
GO

GRANT SELECT ON Object::[quantium].[qpromo].[cdd_raw_cip_segments] TO [sg-rg-data-nonprod-business-internal-read];

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE [qpromo].[cdd_raw_qpromo_key_events]
(
	[date_start] [date],
	[date_end] [date],
	[holiday_name] [varchar](250),
	[applicable_to] [varchar](250),
	[include_flag] [int],
	[md_process_id] [varchar](200),
	[md_source_ts] [datetime2](7),
	[md_created_ts] [datetime2](7),
	[md_source_path] [varchar](200)
)
WITH (DATA_SOURCE = [businessEnrich],LOCATION = N'/internal/quantium/quantium/qpromo/cdd_raw_qpromo_key_events/',FILE_FORMAT = [SynapseParquetFormat])
GO

GRANT SELECT ON Object::[quantium].[qpromo].[cdd_raw_qpromo_key_events] TO [sg-rg-data-nonprod-business-internal-read];

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE [qpromo].[cdd_raw_qpromo_quantium_inbound]
(
	[pw_end_date] [date],
	[pw_start_date] [date],
	[product_id] [varchar](30),
	[primary_win] [varchar](30),
	[product_name] [varchar](200),
	[vendor_name] [varchar](100),
	[vendor_id] [varchar](30),
	[grand_parent_vendor_name] [varchar](100),
	[grand_parent_vendor_id] [varchar](30),
	[category_name] [varchar](100),
	[category_id] [varchar](30),
	[department_name] [varchar](100),
	[department_id] [varchar](30),
	[merchandising_category_name] [varchar](100),
	[merchandising_category_id] [varchar](30),
	[product_profile_group_name] [varchar](100),
	[product_profile_group_id] [varchar](30),
	[brand] [varchar](100),
	[country_id] [varchar](30),
	[country_name] [varchar](100),
	[percent_discount] [decimal](10, 4),
	[npd_flag] [varchar](31),
	[feature_space] [varchar](30),
	[mechanic] [varchar](30),
	[linksave_trigger] [varchar](30),
	[store_count] [int],
	[redemption_rate] [decimal](30, 4),
	[standard_price] [decimal](15, 4),
	[promo_price] [decimal](15, 8),
	[discount] [decimal](15, 8),
	[vat_rate] [float],
	[basket_penetration_uplift] [decimal](38, 4),
	[funding_per_unit] [float],
	[promo_supplier_funding] [float],
	[supplier_funding_percent] [float],
	[unfunded_spend] [float],
	[actual_units] [int],
	[actual_sales] [decimal](18, 2),
	[actual_gp] [decimal](18, 2),
	[gp_margin] [decimal](18, 2),
	[base_units] [decimal](18, 2),
	[base_sales] [decimal](18, 2),
	[base_gp] [decimal](18, 2),
	[units_pure_uplift] [float],
	[sales_pure_uplift] [decimal](18, 2),
	[gp_pure_uplift] [decimal](18, 2),
	[display_units_uplift] [float],
	[display_sales_uplift] [float],
	[display_gp_uplift] [decimal](18, 2),
	[other_units_uplift] [float],
	[other_sales_uplift] [float],
	[other_gp_uplift] [decimal](18, 2),
	[discount_units_uplift] [float],
	[discount_sales_uplift] [float],
	[discount_gp_uplift] [decimal](18, 2),
	[units_forward_buy] [decimal](18, 2),
	[sales_forward_buy] [decimal](18, 2),
	[gp_forward_buy] [decimal](18, 2),
	[units_incrementality] [float],
	[sales_incrementality] [decimal](18, 2),
	[gp_incrementality] [decimal](18, 2),
	[total_units_loss_of_subs] [decimal](18, 2),
	[total_sales_loss_of_subs] [decimal](18, 2),
	[total_gp_loss_of_subs] [decimal](18, 2),
	[final_units_incrementality] [float],
	[final_sales_incrementality] [float],
	[final_profit_incrementality] [decimal](18, 2),
	[classification] [varchar](50),
	[retailer_roi] [float],
	[purchase_cost] [decimal](38, 6),
	[ultra_price_sensitive_uplift_index] [decimal](38, 6),
	[price_sensitive_uplift_index] [decimal](38, 6),
	[mid_market_uplift_index] [decimal](38, 6),
	[upmarket_uplift_index] [decimal](38, 6),
	[high_value_uplift_index] [decimal](38, 6),
	[occasional_high_spend_uplift_index] [decimal](38, 6),
	[frequent_low_spend_uplift_index] [decimal](38, 6),
	[lower_value_uplift_index] [decimal](38, 6),
	[lapsing_uplift_index] [decimal](38, 6),
	[gone_away_uplift_index] [decimal](38, 6),
	[ultra_price_sensitive_uplift_prpn] [decimal](38, 6),
	[price_sensitive_uplift_prpn] [decimal](38, 6),
	[mid_market_uplift_prpn] [decimal](38, 6),
	[upmarket_uplift_prpn] [decimal](38, 6),
	[high_value_uplift_prpn] [decimal](38, 6),
	[occasional_high_spend_uplift_prpn] [decimal](38, 6),
	[frequent_low_spend_uplift_prpn] [decimal](38, 6),
	[lower_value_uplift_prpn] [decimal](38, 6),
	[lapsing_uplift_prpn] [decimal](38, 6),
	[gone_away_uplift_prpn] [decimal](38, 6),
	[md_process_id] [varchar](200),
	[md_source_ts] [datetime2](7),
	[md_created_ts] [datetime2](7),
	[md_source_path] [varchar](200)
)
WITH (DATA_SOURCE = [businessEnrich],LOCATION = N'/internal/quantium/quantium/qpromo/cdd_raw_qpromo_quantium_inbound/',FILE_FORMAT = [SynapseParquetFormat])
GO

GRANT SELECT ON Object::[quantium].[qpromo].[cdd_raw_qpromo_quantium_inbound] TO [sg-rg-data-nonprod-business-internal-read];

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------


