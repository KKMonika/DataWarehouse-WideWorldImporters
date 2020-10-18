CREATE PROCEDURE sp_PerformETL
AS
BEGIN
	SET NOCOUNT ON

	INSERT INTO ETL_process_log(TableName)
	VALUES ('d_Customer')
	DECLARE @ID int
	SELECT @ID = @@IDENTITY
	SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


	--PERFORM ETL HERE
	DECLARE @NUMBEROFRECORDS int
	EXEC @NUMBEROFRECORDS =  sp_PerformETL_Customer

	UPDATE ETL_process_log
	SET ETL_end=getdate(),
	    NumberOfRecords = @NUMBEROFRECORDS
	WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))

	
	INSERT INTO ETL_process_log(TableName)
	VALUES ('d_TransactionType')
	SELECT @ID = @@IDENTITY
	SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


	--PERFORM ETL HERE
	EXEC @NUMBEROFRECORDS =  sp_PerformETL_TransactionType

	UPDATE ETL_process_log
	SET ETL_end=getdate(),
	    NumberOfRecords = @NUMBEROFRECORDS
	WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))


	INSERT INTO ETL_process_log(TableName)
	VALUES ('d_DeliveryMethod')
	SELECT @ID = @@IDENTITY
	SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


	--PERFORM ETL HERE
	EXEC @NUMBEROFRECORDS =  sp_PerformETL_DeliveryMethod

	UPDATE ETL_process_log
	SET ETL_end=getdate(),
	    NumberOfRecords = @NUMBEROFRECORDS
	WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))
	

	INSERT INTO ETL_process_log(TableName)
	VALUES ('d_Rate')
	SELECT @ID = @@IDENTITY
	SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


	--PERFORM ETL HERE
	EXEC @NUMBEROFRECORDS =  sp_PerformETL_Rate

	UPDATE ETL_process_log
	SET ETL_end=getdate(),
	    NumberOfRecords = @NUMBEROFRECORDS
	WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))



	IF object_id('tempdb.dbo.#tmp_all') IS NOT NULL DROP TABLE #tmp_all
	IF object_id('tempdb.dbo.#tmp_inv') IS NOT NULL DROP TABLE #tmp_inv

	SELECT 
	i.InvoiceID as InvoiceID, SUM(il.Quantity) as Quantity, SUM(il.UnitPrice) as UnitPrice, SUM(il.LineProfit) as LineProfit
	into #tmp_inv
	FROM WideWorldImporters.Sales.Invoices i INNER JOIN WideWorldImporters.Sales.InvoiceLines il ON i.InvoiceID =il.InvoiceID
	GROUP BY i.InvoiceID

	SELECT cu.CustomerID as CustomerID,  tt.TransactionTypeID as TransactionTypeID, 
	dm.DeliveryMethodID as DeliveryMethodID, rate.RateID as RateID, ct.CustomerTransactionID as TransactionDateID, t.InvoiceID as InvoiceID,
	ct.CustomerTransactionID CustomerTransactionID,
	t.Quantity  as Quantity, t.UnitPrice as UnitPrice, t.LineProfit as Commisison, ct.AmountExcludingTax as AmountExcludingTax, ct.TaxAmount as TaxAmount, 
	dd.Year as Year, dd.Quarter as Quarter 
	INTO #tmp_all
	FROM  WideWorldImporters.Sales.CustomerTransactions ct	
	INNER JOIN #tmp_inv t
	ON ct.InvoiceID = t.InvoiceID
	INNER JOIN WideWorldImporters.Sales.Customers cu  ON cu.CustomerID = ct.CustomerID 
	INNER JOIN WideWorldImporters.Application.TransactionTypes tt ON tt.TransactionTypeID = ct.TransactionTypeID
	INNER JOIN WideWorldImporters.Application.DeliveryMethods dm ON dm.DeliveryMethodID = cu.DeliveryMethodID
	INNER JOIN dw.dbo.d_date dd ON dd.DATE = ct.TransactionDate
	INNER JOIN WideWorldImporters.Rate.EurUsdRate rate ON rate.RateDate = dd.DATE


	;with cte as (
		SELECT CustomerID, DeliveryMethodID, TransactionTypeID, RateID, Year, Quarter,
				SUM(Quantity) Quantity,SUM(UnitPrice) UnitPrice, SUM(Commisison) Commisison, SUM(AmountExcludingTax) AmountExcludingTax, SUM(TaxAmount) TaxAmount
		FROM #tmp_all
		GROUP BY CustomerID, DeliveryMethodID,TransactionTypeID, RateID, Year, Quarter
		) --SELECT * FROM cte;
		INSERT INTO fact_transaction_temp(d_Customer_skey, d_DeliveryMethod_skey, d_TransactionType_skey, d_Rate_skey, 
					Year, Quarter,  Quantity, UnitPrice, AmountExcludingTax, TaxAmount,Commisison)
		SELECT  dc.skey, dm.skey, tt.skey, r.skey, t.Year, t.Quarter,
			   SUM(Quantity) Quantity,SUM(UnitPrice) UnitPrice, SUM(AmountExcludingTax) AmountExcludingTax,
			   SUM(TaxAmount) TaxAmount, SUM(Commisison) Commisison
		FROM  cte t
		LEFT JOIN d_Customer dc ON dc.ODB_ID = t.CustomerID
		--LEFT JOIN d_Territory dt ON dt.ODB_ID = t.StateProvinceID
		LEFT JOIN d_DeliveryMethod dm ON dm.ODB_ID = t.DeliveryMethodID
		LEFT JOIN d_TransactionType tt ON tt.ODB_ID = t.TransactionTypeID
		LEFT JOIN d_Rate r ON r.ODB_ID = t.RateID
		GROUP BY dc.skey, dm.skey, tt.skey, r.skey, t.Year, t.Quarter
		SET @NUMBEROFRECORDS = @@ROWCOUNT




	BEGIN TRANSACTION SwapTablesForETL
	WITH MARK N'SwapTablesForETL'; 

	EXEC sp_rename 'fact_transaction', 'fact_transaction_old';
	EXEC sp_rename 'fact_transaction_temp', 'fact_transaction';
	EXEC sp_rename 'fact_transaction_old', 'fact_transaction_temp';

	-- Rollback the transaction if there were any errors
	IF @@ERROR <> 0
	BEGIN
	-- Rollback the transaction
	ROLLBACK
	-- Raise an error and return
	RAISERROR ('Error in deleting department in DeleteDepartment.',16, 1)
	RETURN
	END
	-- STEP 4: If we reach this point, the commands completedsuccessfully
	-- Commit the transaction....
	COMMIT TRANSACTION SwapTablesForETL;

	RETURN @NUMBEROFRECORDS


END
