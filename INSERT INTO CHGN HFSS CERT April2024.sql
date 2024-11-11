USE [FTDW_MetaConfig]
GO
INSERT INTO [dbo].[ConfigFileExtractor]
           ([FolderDestination]
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
           ,[HeaderSQLQry])
     VALUES
           ('\\ftdw\qpromo\'
            ,'QPROMO_vwHFSS_Masterfile_Enriched.csv'
			,0
           ,'LDM_PROD_SQLServer'
           ,'SELECT [WIN],[CIN],[PrimeWIN],[ItemNbr],[UPCNbr],[GTIN],[ItemDesc],[ItemShortDesc],[PrimeItemDesc],[HFSS Status] as [HFSS_Status],[Legislative Category] as [Legislative_Category],[HFSS Restriction] as [HFSS_Restriction],[HFSSNumericScore],[ProductType],[Variety],[Weight],[VendorName],[StatusCode],[BrandID],[BrandDivision],[DeptNbr],[DeptDesc],[On GS1] as [On_GS1] FROM [LDM_PROD_SQLServer].[dbo].[vwHFSS_MasterfileEnriched]'
           ,null
           ,null
           ,'13:30:00.0000000'
           ,'4500'
           ,0
           ,'|'
           ,null
           ,'weekly'
           ,6
           ,'-C "28591"'
           ,'\n'
           ,null
           ,null
           ,null)
GO


