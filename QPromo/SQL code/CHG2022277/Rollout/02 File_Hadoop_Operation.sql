/* R.Peake 29/03/2023 */

/****New feed from FTDW to Hadoop  ***/

--1. Check to see if entry already exists, delete if exists 
IF EXISTS
(
	SELECT * 
	FROM FTDW_MetaConfig.dbo.ConfigHadoopFileOperation
	WHERE [FileName] = 'QPROMO_vwFact_Pricing_daily.csv'
)

	BEGIN 
		DELETE FROM FTDW_MetaConfig.dbo.ConfigHadoopFileOperation
	WHERE [FileName] = 'QPROMO_vwFact_Pricing_daily.csv'
END;

--2. Insert values
INSERT INTO FTDW_MetaConfig.dbo.ConfigHadoopFileOperation
			([ConnectionName]  ,[FileExtractorDependencyID] ,[FolderPath]  ,[FileName] 
			,[AppendDateToFileName]	,[IsSourceRecursive]  ,[SourceTimeoutInMin] ,[HadoopFolderPath] 
			,[HadoopFileName] ,[HadoopAppendDateToFileName]  ,[HadoopOperation] ,[IsDestinationOverwrite]
			,[Frequency]  ,[ScheduledDay]  ,[ScheduledStartTime] ,[IsUploadEnabled] ,[CFE_Push_LastCompletedDT] ,[LastCompletedDT] 
			,[ForceRun] ,[HadoopFileType])
values
			('RegionalDataLake_HDUKDEV1',NULL,'\\ftdw\CommercialTransformation\Dev\CSM\','QPROMO_vwFact_Pricing_daily.csv'
			,NULL,0,10,'/user/svc_uk_cust_rdl_dev/sourceFiles/ftdw/QPROMO/'
			,'QPROMO_vwFact_Pricing_daily.csv', NULL, 'CopyToHDFS', 1, 'Daily', NULL, '10:30:00.0000000', 1, NULL, NULL
			, 0, 'File');
;

/*****New feed from LDM_PROD.vwDailySales_Datamart_Dim_Calendar required for CDW project.*****/

--3. Check to see if entry already exists, delete if exists 
IF EXISTS
(
	SELECT * 
	FROM [FTDW_MetaConfig].[dbo].[ConfigHadoopFileOperation]
	WHERE [FileName] = 'QPROMO_vwAnaplan_Bulletin.csv'
)

	BEGIN 
		DELETE FROM [FTDW_MetaConfig].[dbo].[ConfigHadoopFileOperation]
		WHERE [FileName] = 'QPROMO_vwAnaplan_Bulletin.csv' 
END;

--4. Insert values 
INSERT INTO FTDW_MetaConfig.dbo.ConfigHadoopFileOperation
([ConnectionName]  ,[FileExtractorDependencyID] ,[FolderPath]  ,[FileName] ,[AppendDateToFileName]
           ,[IsSourceRecursive]  ,[SourceTimeoutInMin] ,[HadoopFolderPath] ,[HadoopFileName]
           ,[HadoopAppendDateToFileName]  ,[HadoopOperation] ,[IsDestinationOverwrite]
           ,[Frequency]  ,[ScheduledDay]  ,[ScheduledStartTime] ,[IsUploadEnabled]
           ,[CFE_Push_LastCompletedDT]  ,[LastCompletedDT] ,[ForceRun] ,[HadoopFileType])
values
		('RegionalDataLake_HDUKDEV1',NULL,'\\ftdw\CommercialTransformation\Dev\CSM\','QPROMO_vwAnaplan_Bulletin.csv'
		,NULL,0,10,'/user/svc_uk_cust_rdl_dev/sourceFiles/ftdw/QPROMO/'
		,'QPROMO_vwAnaplan_Bulletin.csv', NULL, 'CopyToHDFS', 1, 'Daily', NULL, '10:30:00.0000000', 1, NULL, NULL
		, 0, 'File')
;




