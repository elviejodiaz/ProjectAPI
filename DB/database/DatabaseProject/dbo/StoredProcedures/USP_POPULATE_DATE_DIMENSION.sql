CREATE PROCEDURE [dbo].[USP_POPULATE_DATE_DIMENSION] @StartDate DATE, @EndDate DATE
AS
BEGIN

------------
--Description: Procedure to populate date dimension dbo.DimDate
--No	Date		Description
--01	2025-05-22	Proc creation
------------

	---- Populate the Date Dimension
	--DECLARE @StartDate DATE = '2000-01-01';
	--DECLARE @EndDate   DATE = '2050-12-31';

	--Truncate table before starting
	TRUNCATE TABLE [dbo].[DimDate]

	--Start populating data
	;WITH Dates AS (
		SELECT CAST(@StartDate AS DATE) AS DateValue
		UNION ALL
		SELECT DATEADD(DAY, 1, DateValue)
		FROM Dates
		WHERE DateValue < @EndDate
	)

	INSERT INTO [dbo].[DimDate]
	SELECT
		CONVERT(INT, FORMAT(DateValue, 'yyyyMMdd')) AS DateKey,
		DateValue AS FullDate,
		DAY(DateValue) AS Day,
		RIGHT('0' + CAST(DAY(DateValue) AS VARCHAR(2)), 2) + 
			CASE 
				WHEN DAY(DateValue) IN (11, 12, 13) THEN 'th'
				WHEN RIGHT(CAST(DAY(DateValue) AS VARCHAR),1) = '1' THEN 'st'
				WHEN RIGHT(CAST(DAY(DateValue) AS VARCHAR),1) = '2' THEN 'nd'
				WHEN RIGHT(CAST(DAY(DateValue) AS VARCHAR),1) = '3' THEN 'rd'
				ELSE 'th'
			END AS DaySuffix,
		DATEPART(WEEKDAY, DateValue) AS Weekday,
		DATENAME(WEEKDAY, DateValue) AS WeekdayName,
		CASE WHEN DATEPART(WEEKDAY, DateValue) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend,
		(DATEPART(DAY, DateValue) - 1) / 7 + 1 AS DOWInMonth,
		DATEPART(DAYOFYEAR, DateValue) AS DayOfYear,
		DATEPART(WEEK, DateValue) - DATEPART(WEEK, DATEADD(MONTH, DATEDIFF(MONTH, 0, DateValue), 0)) + 1 AS WeekOfMonth,
		DATEPART(WEEK, DateValue) AS WeekOfYear,
		MONTH(DateValue) AS Month,
		DATENAME(MONTH, DateValue) AS MonthName,
		DATEPART(QUARTER, DateValue) AS Quarter,
		CASE DATEPART(QUARTER, DateValue)
			WHEN 1 THEN 'Q1'
			WHEN 2 THEN 'Q2'
			WHEN 3 THEN 'Q3'
			WHEN 4 THEN 'Q4'
		END AS QuarterName,
		YEAR(DateValue) AS Year,
		RIGHT('0' + CAST(MONTH(DateValue) AS VARCHAR(2)), 2) + CAST(YEAR(DateValue) AS VARCHAR) AS MMYYYY,
		DATEFROMPARTS(YEAR(DateValue), MONTH(DateValue), 1) AS FirstDayOfMonth,
		EOMONTH(DateValue) AS LastDayOfMonth,
		CASE WHEN (YEAR(DateValue) % 4 = 0 AND YEAR(DateValue) % 100 <> 0) OR (YEAR(DateValue) % 400 = 0) THEN 1 ELSE 0 END AS IsLeapYear
	FROM Dates
	OPTION (MAXRECURSION 0)
END
GO
