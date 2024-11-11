/*********************************************************************************************
Script to;
- create QPROMO database in Synpase Analystics
- create raw data tables for ingested files  

................R.Peake 2023/11/15 .............................................................
**********************************************************************************************/
--1. Create database 
--CREATE DATABASE [qpromo]
--GO 

USE [qpromo]
GO 

----2.Create schema

----CREATE SCHEMA [inbound]
----GO 

----CREATE SCHEMA [outbound]
----GO 

----3. Create file format for external table
--IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
--	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
--	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
--	       FORMAT_OPTIONS (
--			 FIELD_TERMINATOR = ',',
--			 FIRST_ROW = 11,
--			 USE_TYPE_DEFAULT = FALSE
--			))
--GO
----4. Create external data source 
--IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'ais_saasllnddtadmdevvuks01_dfs_core_windows_net') 
--	CREATE EXTERNAL DATA SOURCE [ais_saasllnddtadmdevvuks01_dfs_core_windows_net] 
--	WITH (
--		LOCATION = 'abfss://ais@saasllnddtadmdevvuks01.dfs.core.windows.net' 
--	)
--GO

--5. Create Events table 
--CREATE EXTERNAL TABLE inbound.events (
--	[EVENT_NAME] nvarchar(4000),
--	[EVENT_TYPE] nvarchar(4000),
--	[EVENT_GEO_TYPE] nvarchar(4000),
--	[EVENT_GEO_VALUE] nvarchar(4000),
--	[EVENT_START_DATE] nvarchar(4000),
--	[EVENT_END_DATE] nvarchar(4000)
--	)
--	WITH (
--	LOCATION = 'quantium/QPROMO_events.csv',
--	DATA_SOURCE = [ais_saasllnddtadmdevvuks01_dfs_core_windows_net],
--	FILE_FORMAT = [SynapseDelimitedTextFormat]
--	)
--GO

--checks on events table
--SELECT TOP 100 * FROM inbound.events
--GO

IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[inbound].[quantium_qpromo_inbound]') AND type in (N'U'))
DROP EXTERNAL TABLE [inbound].[quantium_qpromo_inbound]
GO


CREATE EXTERNAL TABLE inbound.quantium_qpromo_inbound (
[PW_END_DATE] [nvarchar](4000),
[PW_START_DATE] [nvarchar](4000),
[PRODUCT_ID] [nvarchar](4000),
[PRIMARY_WIN] [nvarchar](4000),
[PRODUCT_NAME] [nvarchar](4000),
[VENDOR_NAME] [nvarchar](4000),
[VENDOR_ID] [nvarchar](4000),
[GRAND_PARENT_VENDOR_NAME] [varchar](100),
[GRAND_PARENT_VENDOR_ID] [nvarchar](4000),
[CATEGORY_NAME] [varchar](100),
[CATEGORY_ID] [varchar](300),
[DEPARTMENT_NAME] [varchar](100),
[DEPARTMENT_ID] [varchar](30),
[MERCHANDISING_CATEGORY_NAME] [varchar](100),
[MERCHANDISING_CATEGORY_ID] [varchar](30),
[PRODUCT_PROFILE_GROUP_NAME] [varchar](100),
[PRODUCT_PROFILE_GROUP_ID] [varchar](30),
[BRAND] [varchar](100),
[COUNTRY_ID] [int],
[COUNTRY_NAME] [varchar](100),
[PERCENT_DISCOUNT] [decimal](10,4),
[NPD_FLAG] [varchar](31),
[FEATURE_SPACE] [varchar](30),
[MECHANIC] [varchar](30),
[LINKSAVE_TRIGGER] [varchar](30),
[STORE_COUNT] [int],
[REDEMPTION_RATE] [decimal](30,4),
[STANDARD_PRICE] [decimal](15,4),
[PROMO_PRICE] [decimal](15,8),
[DISCOUNT] [decimal](15,8),
[VAT_RATE] [float],
[BASKET_PENETRATION_UPLIFT] [decimal](38,4),
[FUNDING_PER_UNIT] [float],
[PROMO_SUPPLIER_FUNDING] [float],
[SUPPLIER_FUNDING_PERCENT] [float],
[UNFUNDED_SPEND] [float],
[ACTUAL_UNITS] [int],
[ACTUAL_SALES] [decimal](18,2),
[ACTUAL_GP] [decimal](18,2),
[GP_MARGIN] [decimal](18,2),
[BASE_UNITS] [decimal](18,2),
[BASE_SALES] [decimal](18,2),
[BASE_GP] [decimal](18,2),
[UNITS_PURE_UPLIFT] [float],
[SALES_PURE_UPLIFT] [decimal](18,2),
[GP_PURE_UPLIFT] [decimal](18,2),
[DISPLAY_UNITS_UPLIFT] [float],
[DISPLAY_SALES_UPLIFT] [float],
[DISPLAY_GP_UPLIFT] [decimal](18,2),
[OTHER_UNITS_UPLIFT] [float],
[OTHER_SALES_UPLIFT] [float],
[OTHER_GP_UPLIFT] [decimal](18,2),
[DISCOUNT_UNITS_UPLIFT] [float],
[DISCOUNT_SALES_UPLIFT] [float],
[DISCOUNT_GP_UPLIFT] [decimal](18,2),
[UNITS_FORWARD_BUY] [decimal](18,2),
[SALES_FORWARD_BUY] [decimal](18,2),
[GP_FORWARD_BUY] [decimal](18,2),
[UNITS_INCREMENTALITY] [float],
[SALES_INCREMENTALITY] [decimal](18,2),
[GP_INCREMENTALITY] [decimal](18,2),
[TOTAL_UNITS_LOSS_OF_SUBS] [decimal](18,2),
[TOTAL_SALES_LOSS_OF_SUBS] [decimal](18,2),
[TOTAL_GP_LOSS_OF_SUBS] [decimal](18,2),
[FINAL_UNITS_INCREMENTALITY] [float],
[FINAL_SALES_INCREMENTALITY] [float],
[FINAL_PROFIT_INCREMENTALITY] [decimal](18,2),
[CLASSIFICATION] [varchar](50),
[RETAILER_ROI] [float],
[PURCHASE_COST] [decimal](38,6),
[ULTRA_PRICE_SENSITIVE_UPLIFT_INDEX] [decimal](38,6),
[PRICE_SENSITIVE_UPLIFT_INDEX] [decimal](38,6),
[MID_MARKET_UPLIFT_INDEX] [decimal](38,6),
[UPMARKET_UPLIFT_INDEX] [decimal](38,6),
[HIGH_VALUE_UPLIFT_INDEX] [decimal](38,6),
[OCCASIONAL_HIGH_SPEND_UPLIFT_INDEX] [decimal](38,6),
[FREQUENT_LOW_SPEND_UPLIFT_INDEX] [decimal](38,6),
[LOWER_VALUE_UPLIFT_INDEX] [decimal](38,6),
[LAPSING_UPLIFT_INDEX] [decimal](38,6),
[GONE_AWAY_UPLIFT_INDEX] [decimal](38,6),
[ULTRA_PRICE_SENSITIVE_UPLIFT_PRPN] [decimal](38,6),
[PRICE_SENSITIVE_UPLIFT_PRPN] [decimal](38,6),
[MID_MARKET_UPLIFT_PRPN] [decimal](38,6),
[UPMARKET_UPLIFT_PRPN] [decimal](38,6),
[HIGH_VALUE_UPLIFT_PRPN] [decimal](38,6),
[OCCASIONAL_HIGH_SPEND_UPLIFT_PRPN] [decimal](38,6),
[FREQUENT_LOW_SPEND_UPLIFT_PRPN] [decimal](38,6),
[LOWER_VALUE_UPLIFT_PRPN] [decimal](38,6),
[LAPSING_UPLIFT_PRPN] [decimal](38,6),
[GONE_AWAY_UPLIFT_PRPN] [decimal](38,6)
)
	WITH (
	LOCATION = '/internal/quantium/qpromo_inbound.parquet',
	DATA_SOURCE = [enrichDataLake],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO

SELECT TOP 100 * FROM inbound.quantium_qpromo_inbound
GO

--

--
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'enrichDataLake') 
	CREATE EXTERNAL DATA SOURCE [enrichDataLake] 
	WITH (
		LOCATION = 'abfss://business@saaslenhdtadmdevvuks01.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE inbound.raw_cip_price_segments (
	[unified_cust_id] nvarchar(4000),
	[seg_value_id] numeric(38,0),
	[segment_id] nvarchar(4000),
	[eff_from_dt] date,
	[eff_to_dt] date
	)
	WITH (
	LOCATION = 'internal/quantium/CIP_price_segments.parquet',
	DATA_SOURCE = [enrichDataLake],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'enrichDataLake') 
	CREATE EXTERNAL DATA SOURCE [enrichDataLake] 
	WITH (
		LOCATION = 'abfss://business@saaslenhdtadmdevvuks01.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE inbound.cip_value_segments (
	[unified_cust_id] nvarchar(4000),
	[seg_value_id] numeric(38,0),
	[segment_id] nvarchar(4000),
	[eff_from_dt] date,
	[eff_to_dt] date
	)
	WITH (
	LOCATION = 'internal/quantium/CIP_value_segments.parquet',
	DATA_SOURCE = [enrichDataLake],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM inbound.cip_value_segments
GO