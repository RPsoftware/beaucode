/*
	TIMINGS 
*/


SELECT 
      [MetadataId], FeedName, FeedLoadType, ExtractionStartTime

      
	  ,max((CopyDurationInSeconds/60) + 1) CopyDurationMins
	  , max(rowsAffected), min(rowsAffected)


  FROM [logging].[AuditLog] a inner join metadata.ConfigFileExtractor c on a.MetadataId = c.id
  
  WHERE Feedname like '%sell%'
  
  group by MetadataId, FeedName, FeedLoadType, ExtractionStartTime

  ORDER BY Metadataid


 /* 
  select * 
  FROM [logging].[AuditLog] a
  where metadataid = '27'
  */