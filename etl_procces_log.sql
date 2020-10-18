CREATE TABLE dw.ETL_process_log(Id int identity,
TableName varchar(200) not null,
ETL_start datetime default getdate(),
ETL_end datetime,
NumberOfRecords int
);

CREATE VIEW vw_ETL_process_log AS
SELECT t.*, datediff(MILLISECOND,t.ETL_start, ETL_end)/1000.0 Duration
FROM dw.ETL_process_log t


DECLARE @ID int
SET @ID = (SELECT count(*) FROM dw.ETL_process_log)+1

PRINT(@ID)