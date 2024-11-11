
CREATE PROCEDURE GetTableDetails
AS
BEGIN
	DECLARE @TableName NVARCHAR(MAX)
	DECLARE @RowCount INT
	DECLARE @UpdatedDate DATETIME 
	DECLARE @ContinueProcessing BIT

	CREATE TABLE #TableDetails (
	TableName NVARCHAR(MAX)
	, [RowCount] INT
	, UpdatedDate DATETIME
	)

	SET @ContinueProcessing = 1 
	WHILE @ContinueProcessing = 1
	BEGIN SELECT TOP 1 @TableName = name from sys.tables where name > coalesce(@TableName, '')
	IF @RowCount = 0
	BEGIN 
		SET @ContinueProcessing = 0
	END 
	ELSE 
	BEGIN
	SET @RowCount = (Select COUNT(*) from sys.dm_db_partition_stats WHERE object_id = OBJECT_ID(@TableName))
	SET @UpdatedDate = (Select modify_date from sys.objects where name = @TableName)
	
	INSERT INTO #TableDetails
	(TableName, [RowCount],UpdatedDate) VALUES (@TableName, @RowCount,@UpdatedDate)
	 END
	 END

	
	SELECT * FROM #TableDetails 

	DROP TABLE #TableDetails
	END

