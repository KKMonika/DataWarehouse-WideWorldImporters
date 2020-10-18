--POPULATING THE DATA WAREHOUSE

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

SELECT * FROM vw_ETL_Process_log


GO
INSERT INTO ETL_process_log(TableName)
VALUES ('d_TransactionType')
DECLARE @ID int
SELECT @ID = @@IDENTITY
SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


--PERFORM ETL HERE
DECLARE @NUMBEROFRECORDS int
EXEC @NUMBEROFRECORDS =  sp_PerformETL_TransactionType

UPDATE ETL_process_log
SET ETL_end=getdate(),
    NumberOfRecords = @NUMBEROFRECORDS
WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))

SELECT * FROM vw_ETL_Process_log


GO
INSERT INTO ETL_process_log(TableName)
VALUES ('d_DeliveryMethod')
DECLARE @ID int
SELECT @ID = @@IDENTITY
SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


--PERFORM ETL HERE
DECLARE @NUMBEROFRECORDS int
EXEC @NUMBEROFRECORDS =  sp_PerformETL_DeliveryMethod

UPDATE ETL_process_log
SET ETL_end=getdate(),
    NumberOfRecords = @NUMBEROFRECORDS
WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))

SELECT * FROM vw_ETL_Process_log


GO
INSERT INTO ETL_process_log(TableName)
VALUES ('d_Rate')
DECLARE @ID int
SELECT @ID = @@IDENTITY
SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


--PERFORM ETL HERE
DECLARE @NUMBEROFRECORDS int
EXEC @NUMBEROFRECORDS =  sp_PerformETL_Rate

UPDATE ETL_process_log
SET ETL_end=getdate(),
    NumberOfRecords = @NUMBEROFRECORDS
WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))

SELECT * FROM vw_ETL_Process_log


GO
INSERT INTO ETL_process_log(TableName)
VALUES ('fact_transaction')
DECLARE @ID int
SELECT @ID = @@IDENTITY
SET @ID = (SELECT COUNT(*) FROM ETL_process_log)+1


--PERFORM ETL HERE
DECLARE @NUMBEROFRECORDS int
EXEC @NUMBEROFRECORDS =  sp_PerformETL

UPDATE ETL_process_log
SET ETL_end=getdate(),
    NumberOfRecords = @NUMBEROFRECORDS
WHERE Id= CONVERT(int, (SELECT COUNT(*) FROM ETL_process_log))-4


SELECT * FROM vw_ETL_Process_log