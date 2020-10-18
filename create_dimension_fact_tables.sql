CREATE TABLE d_Customer(
skey int identity primary key,
CustomerName varchar(100),
ODB_ID int -- Sales.Customer
);

CREATE TABLE d_Territory(
skey int identity primary key,
Country varchar(50),
City varchar(50),
ODB_ID int -- Application.StateProvinces
);

CREATE TABLE d_PaymentMethod(
skey int identity primary key,
PaymentMethodName varchar(50),
ODB_ID int -- Application.PaymentMethod
);

CREATE TABLE d_TransactionType(
skey int identity primary key,
TransactionTypeName varchar(50),
ODB_ID int --Application.TransactionTypes
);

CREATE TABLE d_DeliveryMethod(
skey int identity primary key,
DeliveryMethodName varchar(50),
ODB_ID int --Application.DeliveryMethods
);

CREATE TABLE d_Rate(
skey int identity primary key,
RateDate date,
Rate float,
ODB_ID int --Rate.EurUsdRate
);

CREATE TABLE d_TransactionDate(
skey int identity primary key,
TransactionDate int, --yyyyMMdd
tDate date,
ODB_ID int -- Sales.Invoices.InvoiceDate CHANGE TO CUSTOMERTRANSACTIONS!!!!
);

CREATE TABLE d_date(
		[DateKey] INT primary key, 
		[Date] DATETIME,
		[FullDateUK] CHAR(10), -- Date in dd-MM-yyyy format
		[FullDateUSA] CHAR(10),-- Date in MM-dd-yyyy format
		[DayOfMonth] VARCHAR(2), -- Field will hold day number of Month
		[DaySuffix] VARCHAR(4), -- Apply suffix as 1st, 2nd ,3rd etc
		[DayName] VARCHAR(9), -- Contains name of the day, Sunday, Monday 
		[DayOfWeekUSA] CHAR(1),-- First Day Sunday=1 and Saturday=7
		[DayOfWeekUK] CHAR(1),-- First Day Monday=1 and Sunday=7
		[DayOfWeekInMonth] VARCHAR(2), --1st Monday or 2nd Monday in Month
		[DayOfWeekInYear] VARCHAR(2),
		[DayOfQuarter] VARCHAR(3),
		[DayOfYear] VARCHAR(3),
		[WeekOfMonth] VARCHAR(1),-- Week Number of Month 
		[WeekOfQuarter] VARCHAR(2), --Week Number of the Quarter
		[WeekOfYear] VARCHAR(2),--Week Number of the Year
		[Month] VARCHAR(2), --Number of the Month 1 to 12
		[MonthName] VARCHAR(9),--January, February etc
		[MonthOfQuarter] VARCHAR(2),-- Month Number belongs to Quarter
		[Quarter] CHAR(1),
		[QuarterName] VARCHAR(9),--First,Second..
		[Year] CHAR(4),-- Year value of Date stored in Row
		[YearName] CHAR(7), --CY 2012,CY 2013
		[MonthYear] CHAR(10), --Jan-2013,Feb-2013
		[MMYYYY] CHAR(6),
		[FirstDayOfMonth] DATE,
		[LastDayOfMonth] DATE,
		[FirstDayOfQuarter] DATE,
		[LastDayOfQuarter] DATE,
		[FirstDayOfYear] DATE,
		[LastDayOfYear] DATE,
		[IsHolidayUSA] BIT,-- Flag 1=National Holiday, 0-No National Holiday
		[IsWeekday] BIT,-- 0=Week End ,1=Week Day
		[HolidayUSA] VARCHAR(50),--Name of Holiday in US
		[IsHolidayUK] BIT Null,-- Flag 1=National Holiday, 0-No National Holiday
		[HolidayUK] VARCHAR(50) Null --Name of Holiday in UK
);

CREATE TABLE fact_transaction(
d_Customer_skey int not null references d_Customer(skey),
d_Territory_skey int not null references d_Territory(skey),
d_DeliveryMethod_skey int not null references d_DeliveryMethod(skey),
d_PaymentMethod_skey int not null references d_PaymentMethod(skey),
d_TransactionType_skey int not null references d_TransactionType(skey),
d_TransactionDate_skey int not null references d_TransactionDate(skey),
d_Rate_skey int not null references d_Rate(skey),
Year int not null,
Quarter int not null,
NumberOfTransactions int not null,
Quantity int not null,
AmountExcludingTax decimal not null,
TaxAmount decimal not null,
TransactionAmount decimal not null,
Commisison decimal not null --lineprofit InvoiceLines
);


CREATE TABLE fact_transaction_temp(
d_Customer_skey int not null references d_Customer(skey),
d_Territory_skey int not null references d_Territory(skey),
d_DeliveryMethod_skey int not null references d_DeliveryMethod(skey),
d_PaymentMethod_skey int not null references d_PaymentMethod(skey),
d_TransactionType_skey int not null references d_TransactionType(skey),
d_TransactionDate_skey int not null references d_TransactionDate(skey),
d_Rate_skey int not null references d_Rate(skey),
Year int not null,
Quarter int not null,
NumberOfTransactions int not null,
Quantity int not null,
AmountExcludingTax decimal not null,
TaxAmount decimal not null,
TransactionAmount decimal not null,
Commisison decimal not null --lineprofit InvoiceLines
);