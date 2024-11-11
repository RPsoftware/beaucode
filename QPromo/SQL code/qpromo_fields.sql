--select * from [FTDW_MetaConfig].[dbo].[ConfigHadoopFileOperation] where HadoopFileName like '%QPROMO%'
--select * from [FTDW_MetaConfig].[dbo].[ConfigFileExtractor] where FileName like '%QPROMO%' and extractenabled = 1
--select filename, SQLQry, extractenabled, frequency, weeklydayofweek, starttime from [FTDW_MetaConfig].[dbo].[ConfigFileExtractor] where FileName like '%QPROMO%' and extractenabled = 1
--select filename, isuploadenabled from [FTDW_MetaConfig].[dbo].[ConfigHadoopFileOperation] where HadoopFileName like '%QPROMO%' and isuploadenabled = 1
--select * from [FTDW_MetaConfig].[dbo].[LogFileExtractor] where bcpcommand like '%QPROMO%' order by starttime desc

--select top 10 * from DW_Merch.dbo.vwAnaplan_Bulletin

--exec sp_describe_first_result_set N'select * from [DW_Merch].[dbo].[vwFact_Pricing_daily]'
--exec sp_describe_first_result_set N'select * from DW_Merch.dbo.vwAnaplan_Bulletin'
--exec sp_describe_first_result_set N'select * from LDM_PROD_SQLServer.dbo.vwGMDD_fctPromoStoreVolumes'
--exec sp_describe_first_result_set N'select * from LDM_PROD_SQLServer.dbo.vwGMDD_fctPromoItemVolumes'
--exec sp_describe_first_result_set N'select * from[LDM_PROD_DB2].[dbo].[vwITEM_FUTURE_CHANGE]'
--exec sp_describe_first_result_set N'SELECT * FROM [LDM_PROD].[dbo].[vwSKU_TY_DLY_MUMD]' --can't select 
--exec sp_describe_first_result_set N'select * from[LDM_PROD_DB2].[dbo].[vwITEM_FUTURE_CHANGE]'
--exec sp_describe_first_result_set N'select * from LDM_PROD.dbo.vwAMS_MICOE_GL_Data_GB'
--exec sp_describe_first_result_set N'select * from LDM_PROD_HS.secure.vwAMS_MICOE_Volume_Agreement_GB'
--exec sp_describe_first_result_set N'select * from LDM_PROD_HS.secure.vwAMS_MICOE_Fixed_Agreement_GB'
--exec sp_describe_first_result_set N'select * from DW_MERCH.dbo.vwDim_Calendar_Date'
--exec sp_describe_first_result_set N'select * from DW_MERCH.dbo.vwDim_Calendar_week'
--exec sp_describe_first_result_set N'select * from LDM_PROD.dbo.vwITEM'
--exec sp_describe_first_result_set N'select * from [DW_MERCH].[dbo].[vwDim_Item_Hierarchy]'
--exec sp_describe_first_result_set N'select * from [DW_MERCH].[dbo].[vwFact_Pricing_Promo_Daily]'
--exec sp_describe_first_result_set N'select * from [LDM_PROD_TD_GB_WM_VM].[rewards].[vwSKU_DLY_POS_UNPIVOT_ADJ]' 
--exec sp_describe_first_result_set N'select * from [LDM_PROD_TD_GB_WM_VM].[dbo].[vwITEM]' 
--exec sp_describe_first_result_set N'select * from [LDM_PROD_SQLServer].[dbo].[vwGMDD_dimPromoDetail]' 

--select MAX(LDM_DT_LastLoad), MIN(ldm_dt_lastload)  from LDM_PROD_HS.secure.vwAMS_MICOE_Volume_Agreement_GB ;
--select top 50000 *  from LDM_PROD_HS.secure.vwAMS_MICOE_Volume_Agreement_GB where LDM_DT_LastLoad > GETDATE() - 1096
--select MAX(LDM_DT_LastLoad), MIN(ldm_dt_lastload) from LDM_PROD_HS.secure.vwAMS_MICOE_Fixed_Agreement_GB;
--select MAX(LDM_DT_LastLoad), MIN(ldm_dt_lastload) from LDM_PROD.dbo.vwAMS_MICOE_GL_Data_GB where LDM_DT_LastLoad > GETDATE() - 1096;
--select * from LDM_PROD.dbo.vwAMS_MICOE_GL_Data_GB;

--select Department_Desc, COUNT(*) from DW_Merch.dbo.vwAnaplan_Bulletin group by Department_Desc

--select COUNT(*) from DW_MERCH.dbo.vwDim_Calendar_Date;
--select COUNT(*) from DW_MERCH.dbo.vwDim_Calendar_week; 

--SELECT * FROM [DW_MERCH].[dbo].[vwDim_Item_Hierarchy];

--SELECT COUNT(*) FROM [DW_Merch].[dbo].[vwFact_Pricing_daily]    

--SELECT * FROM [LDM_PROD_TD_GB_WM_VM].[dbo].[vwITEM] where (DEPT_NBR IN (48, 50, 51) AND VNPK_WEIGHT_FMT_CD <> 'V' ) OR DEPT_NBR NOT IN (48, 50, 51)   

--SELECT * FROM [LDM_PROD_TD_GB_WM_VM].[rewards].[vwSKU_DLY_POS_UNPIVOT_ADJ] where LDM_DT_LastLoad >= GETDATE() - 90

--where Date_Key like '2023%'

--select MIN(date_key) FROM [DW_Merch].[dbo].[vwFact_Pricing_daily]

--SELECT * FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where Date_Key like '2022%'

--with A as (
--SELECT *, ROW_NUMBER() OVER (PARTITION BY fact_key, item_key ORDER BY  desc) as row_num
--FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where Collection_Date = '2023-04-26'
--) 

--select * from A where row_num = 1 

--where comp_key = '27' and [Trad_Area_Key] not in(6,7)
--and Collection_Date BETWEEN (try_convert(date, (GETDATE()- 1096), 103)) 
--AND (try_convert(date, (GETDATE() - 915), 103));

SELECT COUNT(*), MIN(Collection_Date), MAX(collection_date) FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where comp_key = '27' and [Trad_Area_Key] not in(6,7)
and Collection_Date between (try_convert(date, (GETDATE()- 914), 103)) AND (try_convert(date, (GETDATE() - 732), 103));

SELECT COUNT(*), MIN(Collection_Date), MAX(collection_date) FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where comp_key = '27' and [Trad_Area_Key] not in(6,7)
and Collection_Date between (try_convert(date, (GETDATE()- 731), 103)) AND (try_convert(date, (GETDATE() - 549), 103));

SELECT COUNT(*), MIN(Collection_Date), MAX(collection_date) FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where comp_key = '27' and [Trad_Area_Key] not in(6,7)
and Collection_Date between (try_convert(date, (GETDATE() - 548), 103)) AND (try_convert(date, (GETDATE() - 366), 103));

SELECT COUNT(*), MIN(Collection_Date), MAX(collection_date) FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where comp_key = '27' and [Trad_Area_Key] not in(6,7)
and Collection_Date between (try_convert(date, (GETDATE() - 365), 103)) AND (try_convert(date, (GETDATE() - 183), 103));

SELECT * FROM [DW_MERCH].[dbo].[vwFact_Pricing_Daily] where comp_key = '27' and [Trad_Area_Key] not in(6,7)
and Collection_Date between (try_convert(date, (GETDATE() - 182), 103)) AND (try_convert(date, (GETDATE() - 0), 103));


--select --MIN(ldm_dt_lastload), MAX(ldm_dt_lastload), MIN(createddate), MAX(createddate) 
--count(*)  from [LDM_PROD_SQLServer].[dbo].[vwGMDD_dimPromoDetail] where createddate <= '2023-04-06'

--SELECT MAX(Loaded_Date),MIN(promo_period_start_date), max(promo_period_start_date), count(*)
--FROM [DW_Merch].[dbo].[vwAnaplan_Bulletin] where Promo_Period_Start_Date <= '2023-06-29'

--select COUNT(*) FROM [DW_Merch].[dbo].[vwAnaplan_Bulletin] where Promo_Period_Start_Date <= '2023-06-29'

--select * from [LDM_PROD_TD_GB_WM_VM].[rewards].[vwSKU_DLY_POS_UNPIVOT_ADJ] sdp
--where sdp.GREGORIAN_DATE >='2022-09-18' AND sdp.GREGORIAN_DATE <='2022-09-20'

with a as (SELECT gregorian_date, STORE_NBR, ITEM_NBR, SELL_PRICE, QTY, SALES_AMT FROM [LDM_PROD_TD_GB_WM_VM].[rewards].[vwSKU_DLY_POS_UNPIVOT_ADJ] where GREGORIAN_DATE >='2023-03-01') 
select COUNT_BIG(ITEM_NBR) from a



