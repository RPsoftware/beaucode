/* R.Peake 29/03/2023*/

/****New feed from [FTDW] required for QPROMO project ***/

--1. Check to see if entry already exists
IF EXISTS
(
	SELECT * 
	FROM [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
	WHERE [FileName] = 'QPROMO_vwFact_Pricing_daily.csv'
)

	BEGIN 
		DELETE FROM [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
		WHERE [FileName] = 'QPROMO_vwFact_Pricing_daily.csv'
END;

--2. Insert values
INSERT INTO [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
			([FolderDestination] ,[FileName] ,[AppendDateToFileName] ,[SourceDB] ,[SQLQry]  ,[Push_LastComplete_DT]
			,[LDM_StgToProdID_Dependency] ,[StartTime]  ,[BCPTimeoutOverrideSecs] ,[ExtractEnabled]
			,[ColumnDelimiter]  ,[DMExtractor_Dependency]  ,[Frequency] ,[WeeklyDayOfWeek]
			,[BCPCodePage]  ,[BCPRowTerminator]  ,[ZipFile]  ,[ZipFileProcessPath])

values      ('\\ftdw\CommercialTransformation\Dev\CSM\' ,'QPROMO_vwFact_Pricing_daily.csv'
			,0    ,'DW_Merch'
			,'SELECT * FROM [DW_Merch].[dbo].[vwFact_Pricing_daily]'
			,NULL   ,NULL ,'10:00:00.000'  ,900,   1  ,'|'  ,NULL  ,NULL  ,NULL  ,'-C "28591"' ,'\n'  ,NULL ,NULL)
;		 



--3. Check to see if entry already exists

IF EXISTS
(
	SELECT * 
	FROM [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
	WHERE [FileName] = 'QPROMO_vwAnaplan_Bulletin.csv'
)

	BEGIN 
		DELETE FROM [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
		WHERE [FileName] = 'QPROMO_vwAnaplan_Bulletin.csv'
END;

--4. Insert values
INSERT INTO [FTDW_MetaConfig].[dbo].[ConfigFileExtractor]
			([FolderDestination] ,[FileName] ,[AppendDateToFileName] ,[SourceDB] ,[SQLQry]  ,[Push_LastComplete_DT]
			,[LDM_StgToProdID_Dependency] ,[StartTime]  ,[BCPTimeoutOverrideSecs] ,[ExtractEnabled]
			,[ColumnDelimiter]  ,[DMExtractor_Dependency]  ,[Frequency] ,[WeeklyDayOfWeek]
			,[BCPCodePage]  ,[BCPRowTerminator]  ,[ZipFile]  ,[ZipFileProcessPath])

values      ('\\ftdw\CommercialTransformation\Dev\CSM\' ,'QPROMO_vwAnaplan_Bulletin.csv'
			,0    ,'DW_Merch'
			,'SELECT * FROM [DW_Merch].[dbo].[vwAnaplan_Bulletin] '
			,NULL   ,NULL ,'10:00:00.000'  ,900,   1  ,'|'  ,NULL  ,NULL  ,NULL  ,'-C "28591"' ,'\n'  ,NULL ,NULL)
;		 

