/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [id]
      ,[FeedName]
      ,[FeedLoadType]
      ,[SourceServer]
      ,[SourceDB]
      ,[SourceSchema]
      ,[SourceTable]
      ,[SourceColumns]
      ,[SourceHashColumns]
      ,[SourceWatermarkColumn]
      ,[SourceWatermarkColumnLag]
      ,[SourceSQLQuery]
      ,[DestinationBlobContainer]
      ,[DestinationBlobFolder]
      ,[DestinationFileName]
      ,[DestinationFileColumnDelimiter]
      ,[DestinationFileEscapeChar]
      ,[LastSuccessfulDataExtrnDate]
      ,[LastExtrnStatus]
      ,[LastFailedDataExtrnDate]
      ,[ExtractionStartTime]
      ,[FunctionalAreaId]
      ,[IsEnabled]
      ,[ScheduleFrequency]
      ,[SatInd]
      ,[SunInd]
      ,[MonInd]
      ,[TueInd]
      ,[WedInd]
      ,[ThuInd]
      ,[FriInd]
      ,[MonthInd]
      ,[CreatedDate]
      ,[notebookPath]
      ,[SourceLSId]
      ,[DestinationLSId]
  FROM [metadata].[ConfigFileExtractor]

select single_profile_id, replace(translate(contact_first_name,',~#$%\}<>{?*!"','!!!!!!!!!!!!!!'),'!','') contact_first_name, email, is_email_invalid,
    is_string_excluded, on_blacklist, hard_bounce, hard_bounce_date, is_suspended, is_valid_contact, permission_value,
    permission_date, suppression_value, suppression_date, is_email_contactable, international_customer, last_open_email_date, gdpr_engaged, 
	global_preference_flag, snapshot_date 
from [gb_customer_data_domain_secured_rpt].[cdd_rpt_ft_pc_global_contactable_email]
where (last_open_email_date > dateadd(d, -480, getdate()) AND is_email_contactable = 'Y')
	OR permission_date >= dateadd(d, -60, getdate()) 
	OR suppression_date >= dateadd(d, -120, getdate()) 
	OR hard_bounce_date >= dateadd(d, -120, getdate()) 
	OR on_blacklist = 'Y' 