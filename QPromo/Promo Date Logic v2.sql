
--LINKSAVE PRODUCT LEVEL

--The start date is unique for every promo_ID_Key so taking the min and max colleciton date when promo type removes multiple end dates
DROP TABLE ##Linksaves_Prod
SELECT * INTO ##Linksaves_Prod
FROM (SELECT Item_barcode, Promo_ID_KEY, 
		MIN(Collection_Date) AS Linksave_prod_Start, 
		MAX(Collection_Date) AS Linksave_Prod_End,
		MIN(CASE WHEN Supermarket_Linksave_Strt_Dt IS NOT NULL THEN Collection_Date ELSE NULL END) AS Supermarket_Linksave_prod_Start, 
		MAX(CASE WHEN Supermarket_Linksave_Strt_Dt IS NOT NULL THEN Collection_Date ELSE NULL END) AS Supermarket_Linksave_prod_End, 
		MIN(CASE WHEN Convenience_Linksave_Strt_Dt IS NOT NULL THEN Collection_Date ELSE NULL END) AS Convenience_Linksave_prod_Start, 
		MAX(CASE WHEN Convenience_Linksave_Strt_Dt IS NOT NULL THEN Collection_Date ELSE NULL END) AS Convenience_Linksave_prod_End
	  FROM DW_MERCH.DBO.vwFact_pricing_promo_Daily
	  WHERE asda_promo_type IN ('Linksave','Linksave and Rollback')
	  GROUP BY Item_barcode, Promo_ID_KEY) AS A

--(78018 rows affected)

--LINKSAVE PROMO LEVEL

--Take the min and max start and end dates to give us the promotion level view
DROP TABLE ##Linksaves_Promo
SELECT * INTO ##Linksaves_Promo
FROM (SELECT  Promo_ID_KEY,  
			MIN(Linksave_Strt_Dt) AS Linksave_Promo_Strt_Dt,
			MAX(Linksave_End_Dt) AS Linksave_Promo_End_Dt,
			MIN(Supermarket_Linksave_Strt_Dt) AS Supermarket_Linksave_Promo_Strt_Dt,
			MAX(Supermarket_Linksave_End_Dt) AS Supermarket_Linksave_Promo_End_Dt,
			MIN(Convenience_Linksave_Strt_Dt) AS Convenience_Linksave_Promo_Strt_Dt,
			MAX(Convenience_Linksave_End_Dt) AS Convenience_Linksave_Promo_End_Dt
	  FROM DW_MERCH.DBO.vwFact_pricing_promo_Daily
	  WHERE asda_promo_type IN ('Linksave','Linksave and Rollback')
	  GROUP BY Promo_ID_KEY) AS A

--(2918 rows affected)

--ROLLBACKS
--With rollbacks we need to take into account the Linksave and Rollback data and we need to create a new promo id key for rollbacks only
DROP TABLE ##Rollback_Prod
SELECT * INTO ##Rollback_Prod
FROM (
Select Item_barcode,
		Collection_Date,
		CASE WHEN Rollback_Flag = 'Y' THEN MAX(Promo_ID_Key2) over (partition by name_rowid) Else null end as Promo_ID_Key_RB,
		CASE WHEN Rollback_Flag = 'Y' THEN MIN(Collection_Date) over (partition by name_rowid) Else null end as Rollback_prod_Start,
		CASE WHEN Rollback_Flag = 'Y' THEN MAX(Collection_Date) over (partition by name_rowid) Else null end as Rollback_Prod_End
from (
	SELECT *,  max(case when Promo_ID_Key2 is not null then Concat_Data end) over (order by Concat_Data) as name_rowid
	FROM (
			SELECT * , CONCAT(Item_barcode,date_key) AS Concat_Data,
			CASE WHEN  LAG(Rollback_Flag,1) OVER (PARTITION BY Item_Barcode ORDER BY collection_Date) <> Rollback_Flag AND Rollback_Flag = 'Y' THEN Promo_ID_Key ELSE null END
				AS Promo_ID_Key2
			FROM DW_MERCH.DBO.vwFact_pricing_promo_Daily
			) AS T1
--WHERE item_barcode = 1834175105
	) as t2
	WHERE Rollback_Flag = 'Y'
) AS T3
order by Collection_Date




--Pull all the data together and create linksave only Promo ID Key
DROP TABLE ##PromoDaily_New
SELECT A.*, 
		B.Linksave_prod_Start,
		B.Linksave_Prod_End,
		C.Linksave_Promo_Strt_Dt,
		C.Linksave_Promo_End_Dt,
 Rollback_prod_Start,
 Rollback_Prod_End,
 Supermarket_Linksave_prod_Start, 
Supermarket_Linksave_prod_End, 
 Convenience_Linksave_prod_Start, 
 Convenience_Linksave_prod_End,
Supermarket_Linksave_Promo_Strt_Dt,
Supermarket_Linksave_Promo_End_Dt,
Convenience_Linksave_Promo_Strt_Dt,
Convenience_Linksave_Promo_End_Dt
		,CASE WHEN Asda_promo_type IN ('Linksave','Linksave and Rollback') THEN A.Promo_ID_KEY ELSE NULL END AS Promo_ID_KEY_LS,
	Promo_ID_Key_RB
INTO ##PromoDaily_New
FROM DW_Merch.DBO.vwFact_pricing_promo_Daily AS A
LEFT JOIN ##Linksaves_Prod AS B
ON A.Collection_Date BETWEEN Linksave_prod_Start AND Linksave_Prod_End
AND A.Item_barcode = B.Item_barcode
AND A.promo_ID_Key = B.Promo_ID_Key
LEFT JOIN ##Linksaves_Promo AS C
ON A.Collection_Date BETWEEN Linksave_Promo_Strt_Dt AND Linksave_Promo_End_Dt
AND A.Promo_ID_key = C.Promo_ID_key
LEFT JOIN ##Rollback_Prod AS D
ON A.Collection_Date = D.Collection_Date
AND A.Item_barcode = D.Item_barcode
