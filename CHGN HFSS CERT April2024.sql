/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [WIN],[CIN],[PrimeWIN],[ItemNbr],[UPCNbr],[GTIN],[ItemDesc],[ItemShortDesc],[PrimeItemDesc],[HFSS Status] as [HFSS_Status],[Legislative Category] as [Legislative_Category],[HFSS Restriction] as [HFSS_Restriction],[HFSSNumericScore],[ProductType],[Variety],[Weight],[VendorName],[StatusCode],[BrandID],[BrandDivision],[DeptNbr],[DeptDesc],[On GS1] as [On_GS1] FROM [LDM_PROD_SQLServer].[dbo].[vwHFSS_MasterfileEnriched]