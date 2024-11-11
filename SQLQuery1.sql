/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [ID]
      ,[FolderDestination]
      ,[FileName]
      ,[AppendDateToFileName]
      ,[SourceDB]
      ,[SQLQry]
      ,[Push_LastComplete_DT]
      ,[LDM_StgToProdID_Dependency]
      ,[StartTime]
      ,[BCPTimeoutOverrideSecs]
      ,[ExtractEnabled]
      ,[ColumnDelimiter]
      ,[DMExtractor_Dependency]
      ,[Frequency]
      ,[WeeklyDayOfWeek]
      ,[BCPCodePage]
      ,[BCPRowTerminator]
      ,[ZipFile]
      ,[ZipFileProcessPath]
      ,[DayOfMonth]
  FROM [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
  where filename like 'QPROMO%' and ExtractEnabled = 1
  
  select dbparentstorekey, dbparentfloorplankey, load_date_time from [Supply_MPI].[dbo].[vw_ix_str_store_floorplan] 

  update [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
  set SQLQry = 'select dbkey, CASE WHEN PATINDEX(''%[^0]%'', upc + ''.'') > LEN(upc) THEN RIGHT(upc, 1) ELSE SUBSTRING(upc, PATINDEX(''%[^0]%'', upc + ''.''), LEN(upc)) END AS upc, id, [name] from [Supply_MPI].[dbo].[vw_ix_spc_product] ', WeeklyDayOfWeek = 1
  where FileName = 'QPROMO_vw_ix_spc_product.csv' and ExtractEnabled = 1

select dbkey
,CASE WHEN PATINDEX('%[^0]%', upc + '.') > LEN(upc) THEN RIGHT(upc, 1) ELSE SUBSTRING(upc, PATINDEX('%[^0]%', upc + '.'), LEN(upc))
END  AS upc
, id, [name] , pf.*
from [Supply_MPI].[dbo].[vw_ix_spc_product] p inner join (select DBPARENTPLANOGRAMKEY, LINEAR, [SQUARE], dbparentproductkey from [Supply_MPI].[dbo].[vw_ix_spc_performance] ) pf
on pf.dbparentproductkey = p.dbkey