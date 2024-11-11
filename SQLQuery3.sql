

select * from [FTDW_MetaConfig].[dbo].[ConfigFileExtractor] where filename like '%sku%'

SELECT gregorian_date, STORE_NBR, ITEM_NBR, SELL_PRICE, QTY, SALES_AMT 
FROM [LDM_PROD_TD_GB_WM_VM].[rewards].[vwSKU_DLY_POS_UNPIVOT_ADJ] 
where GREGORIAN_DATE >= '2022-12-01' AND GREGORIAN_DATE <= '2022-12-31'