CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @load_start_time DATETIME, @load_end_time DATETIME
	BEGIN TRY
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

	SET @load_start_time = GETDATE();

	set @start_time = GETDATE();
		PRINT '>> Truncating TABLE: bronze.orders';
		TRUNCATE TABLE bronze.orders;

		PRINT '>> Inserting Data Into: bronze.orders';
		BULK INSERT bronze.orders
		FROM 'D:\Project datasets\Maven+Fuzzy+Factory\orders.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	set @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------';

	set @start_time = GETDATE();
		PRINT '>> Truncating TABLE: order_item_refunds'
		TRUNCATE TABLE bronze.order_item_refunds;
	
		PRINT '>> Loading Data Into: order_item_refunds'
		BULK INSERT bronze.order_item_refunds
		FROM 'D:\Project datasets\Maven+Fuzzy+Factory\order_item_refunds.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	set @end_time = GETDATE();
	print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------';

	set @start_time = GETDATE();
		PRINT '>> Truncating TABLE: order_items.'
		TRUNCATE TABLE bronze.order_items;

		PRINT '>> Loading Data Into: order_items.'
		BULK INSERT bronze.order_items
		FROM 'D:\Project datasets\Maven+Fuzzy+Factory\order_items.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	set @end_time = GETDATE();
	print '>> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------';

	set @start_time = GETDATE();
		print '>> Truncating Table: products'
		TRUNCATE TABLE bronze.products;

		print '>> Load Data Into: products'
		BULK INSERT bronze.products
		FROM 'D:\Project datasets\Maven+Fuzzy+Factory\products.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	set @end_time = GETDATE();
	print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------';

	set @start_time = GETDATE();
		print '>> Truncating Table: website_pageviews';
		TRUNCATE TABLE bronze.website_pageviews;

		print '>> Load Data Into: website_pageviews';
		BULK INSERT bronze.website_pageviews
		FROM 'D:\Project datasets\Maven+Fuzzy+Factory\website_pageviews.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0A',
			TABLOCK,
			CODEPAGE = '65001'
		);
	set @end_time = GETDATE();
	print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------';

	set @start_time = GETDATE();
		print '>> Truncating TABLE: website_sessions';
		TRUNCATE TABLE bronze.website_sessions;

		print '>> Load Data Into: website_sessions';
		BULK INSERT bronze.website_sessions
		FROM 'D:\Project datasets\Maven+Fuzzy+Factory\website_sessions.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
	set @end_time = GETDATE();
	print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '---------------------';

	SET @load_end_time = GETDATE();
	print '=================================';
	print '>> Loading Bronze Procedure Finished';
	Print '>> Bronze Load Duration: ' + CAST(DATEDIFF(second, @load_start_time, @load_end_time) AS NVARCHAR) + ' seconds';
	print '=================================';

	END TRY
	BEGIN CATCH
		PRINT '=================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=================================';
	END CATCH
END