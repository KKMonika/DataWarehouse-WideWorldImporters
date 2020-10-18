--ETL PROCESS


CREATE PROCEDURE sp_PerformETL_Customer
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO d_Customer(CustomerName, ODB_ID)
	SELECT c.CustomerName, c.CustomerID
	FROM WideWorldImporters.Sales.Customers c 
	LEFT JOIN d_Customer dc ON c.CustomerID = dc.ODB_ID
	WHERE dc.skey IS NULL

	UPDATE d_Customer
	SET CustomerName = c.CustomerName
	FROM WideWorldImporters.Sales.Customers c 
	LEFT JOIN d_Customer dc ON c.CustomerID = dc.ODB_ID
	WHERE dc.CustomerName != c.CustomerName
END

GO
CREATE PROCEDURE sp_PerformETL_Territory
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO d_Territory(Country, City, ODB_ID)
	SELECT co.CountryName, ci.CityName, sp.StateProvinceID
	FROM WideWorldImporters.Application.StateProvinces sp LEFT JOIN 
		 WideWorldImporters.Application.Cities ci ON sp.StateProvinceID = ci.StateProvinceID LEFT JOIN
		 WideWorldImporters.Application.Countries co ON sp.CountryID = co.CountryID LEFT JOIN d_Territory dt ON
		 ODB_ID = sp.StateProvinceID
		 WHERE dt.skey IS NULL

	UPDATE d_Territory
	SET Country = co.CountryName,
		City = ci.CityName
	FROM WideWorldImporters.Application.StateProvinces sp LEFT JOIN 
		 WideWorldImporters.Application.Cities ci ON sp.StateProvinceID = ci.StateProvinceID LEFT JOIN
		 WideWorldImporters.Application.Countries co ON sp.CountryID = co.CountryID LEFT JOIN 
		 d_Territory dt ON dt.ODB_ID = sp.StateProvinceID
	WHERE dt.Country != co.CountryName AND dt.City != ci.CityName
END

-- GO
-- CREATE PROCEDURE sp_PerformETL_PaymentMethod
-- AS
-- BEGIN
-- 	SET NOCOUNT ON
-- 	INSERT INTO d_PaymentMethod(PaymentMethodName, ODB_ID)
-- 	SELECT pm.PaymentMethodName, pm.PaymentMethodID
-- 	FROM WideWorldImporters.Application.PaymentMethods pm  LEFT JOIN d_PaymentMethod dt ON
-- 		 ODB_ID = pm.PaymentMethodID
-- 		 WHERE dt.skey IS NULL

-- 	UPDATE d_PaymentMethod
-- 	SET PaymentMethodName = pm.PaymentMethodName
-- 	FROM WideWorldImporters.Application.PaymentMethods pm
-- 	WHERE ODB_ID = pm.PaymentMethodID 
-- 	AND d_PaymentMethod.PaymentMethodName != pm.PaymentMethodName
-- END

GO
CREATE PROCEDURE sp_PerformETL_TransactionType
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO d_TransactionType(TransactionTypeName, ODB_ID)
	SELECT tt.TransactionTypeName, tt.TransactionTypeID
	FROM WideWorldImporters.Application.TransactionTypes tt  LEFT JOIN d_TransactionType dt ON
		 ODB_ID = tt.TransactionTypeID
		 WHERE dt.skey IS NULL

	UPDATE d_TransactionType
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
	INSERT INTO d_DeliveryMethod(DeliveryMethodName, ODB_ID)
	SELECT dm.DeliveryMethodName, dm.DeliveryMethodID
	FROM WideWorldImporters.Application.DeliveryMethods dm  LEFT JOIN d_DeliveryMethod dt ON
		 ODB_ID = dm.DeliveryMethodID
		 WHERE dt.skey IS NULL

	UPDATE d_DeliveryMethod
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
	INSERT INTO d_Rate(RateDate, Rate,  ODB_ID)
	SELECT r.RateDate, r.RateOpen, r.RateID
	FROM WideWorldImporters.Rate.EurUsdRate r  LEFT JOIN d_Rate dt ON
		 ODB_ID = r.RateID
		 WHERE dt.skey IS NULL

	UPDATE d_Rate
	SET RateDate = r.RateDate,
		Rate = r.RateOpen
	FROM WideWorldImporters.Rate.EurUsdRate r
	WHERE ODB_ID = r.RateID
	AND d_Rate.RateDate != r.RateDate
	AND d_Rate.Rate != r.RateOpen
END

CREATE PROCEDURE sp_PerformETL_TransactionDate
AS
BEGIN
	SET NOCOUNT ON
	INSERT INTO d_TransactionDate(TransactionDate, tDate, ODB_ID)
	SELECT dd.DateKey, ct.TransactionDate, ct.CustomerTransactionID
	FROM WideWorldImporters.Sales.CustomerTransactions ct 
	LEFT JOIN d_date dd ON ct.TransactionDate = dd.Date
	LEFT JOIN d_TransactionDate dt ON dt.tDate = dd.Date
	WHERE dt.skey IS NULL

	UPDATE d_TransactionDate
	SET TransactionDate = dd.DateKey,
		tDate = ct.TransactionDate
	FROM WideWorldImporters.Sales.CustomerTransactions ct
	LEFT JOIN d_date dd ON ct.TransactionDate = dd.Date
	WHERE ODB_ID = ct.CustomerTransactionID
	AND tDate != ct.TransactionDate
END
