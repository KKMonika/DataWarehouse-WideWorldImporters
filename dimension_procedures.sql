--ETL PROCESS


CREATE PROCEDURE sp_PerformETL_Customer
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO dw.d_Customer(CustomerName, ODB_ID)
	SELECT c.CustomerName, c.CustomerID
	FROM WideWorldImporters.Sales.Customers c 
	LEFT JOIN dw.d_Customer dc ON c.CustomerID = dc.ODB_ID
	WHERE dc.skey IS NULL

	UPDATE dw.d_Customer
	SET CustomerName = c.CustomerName
	FROM WideWorldImporters.Sales.Customers c 
	LEFT JOIN dw.d_Customer dc ON c.CustomerID = dc.ODB_ID
	WHERE dc.CustomerName != c.CustomerName
END



GO
CREATE PROCEDURE sp_PerformETL_TransactionType
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO dw.d_TransactionType(TransactionTypeName, ODB_ID)
	SELECT tt.TransactionTypeName, tt.TransactionTypeID
	FROM WideWorldImporters.Application.TransactionTypes tt  LEFT JOIN dw.d_TransactionType dt ON
		 ODB_ID = tt.TransactionTypeID
		 WHERE dt.skey IS NULL

	UPDATE dw.d_TransactionType
	SET TransactionTypeName = tt.TransactionTypeName
	FROM WideWorldImporters.Application.TransactionTypes tt
	WHERE ODB_ID = tt.TransactionTypeID
	AND d_TransactionType.TransactionTypeName != tt.TransactionTypeName
END

GO
CREATE PROCEDURE sp_PerformETL_DeliveryMethod
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO dw.d_DeliveryMethod(DeliveryMethodName, ODB_ID)
	SELECT dm.DeliveryMethodName, dm.DeliveryMethodID
	FROM WideWorldImporters.Application.DeliveryMethods dm  LEFT JOIN dw.d_DeliveryMethod dt ON
		 ODB_ID = dm.DeliveryMethodID
		 WHERE dt.skey IS NULL

	UPDATE dw.d_DeliveryMethod
	SET DeliveryMethodName = dm.DeliveryMethodName
	FROM WideWorldImporters.Application.DeliveryMethods dm
	WHERE ODB_ID = dm.DeliveryMethodID
	AND d_DeliveryMethod.DeliveryMethodName != dm.DeliveryMethodName
END

--FIRST QUICK FIX
-- ALTER TABLE WideWorldImporters.Rate.EurUsdRate ADD RateID int identity;

GO
CREATE PROCEDURE sp_PerformETL_Rate
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO dw.d_Rate(RateDate, Rate,  ODB_ID)
	SELECT r.RateDate, r.RateOpen, r.RateID
	FROM WideWorldImporters.Rate.EurUsdRate r  LEFT JOIN dw.d_Rate dt ON
		 ODB_ID = r.RateID
		 WHERE dt.skey IS NULL

	UPDATE dw.d_Rate
	SET RateDate = r.RateDate,
		Rate = r.RateOpen
	FROM WideWorldImporters.Rate.EurUsdRate r
	WHERE ODB_ID = r.RateID
	AND d_Rate.RateDate != r.RateDate
	AND d_Rate.Rate != r.RateOpen
END

