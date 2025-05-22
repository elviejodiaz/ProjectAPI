CREATE VIEW [dbo].[VW_HIRES_PER_DEPARTMENT_EXCEEDING_YEAR_MEAN]
AS
------------
--Description: View that returns the number of employees hired per department and year, which are exceeding the year mean
--			   This is to solve second requirement on Challenge #2
--No	Date		Description
--01	2025-05-22	View creation
----------
WITH CTE AS (
	SELECT 
		  D.[YEAR]
		 ,A.[department_id]
		 ,B.[DEPARTMENT]
		 ,DEPARTMENT_HIRES_YEAR	= COUNT(A.[ID])
	FROM dbo.HiredEmployees A
		INNER JOIN dbo.Departments B
			ON A.department_id = B.id
		INNER JOIN dbo.DimDate D
			ON CAST(A.[datetime] AS DATE) = D.FullDate
	GROUP BY
		 D.[YEAR]
		 ,A.[department_id]
		 ,B.[DEPARTMENT]
)

, CTE_YEAR_MEAN AS (
	SELECT
		 [YEAR]
		,[department_id]
		,[DEPARTMENT]
		,DEPARTMENT_HIRES_YEAR
		,MEAN_YEAR				= AVG(CAST(DEPARTMENT_HIRES_YEAR AS DECIMAL(38,6))) OVER (PARTITION BY [YEAR])
	FROM CTE
)	--SELECT * FROM CTE_YEAR_MEAN
	
, CTE_DEPARTMENTS_EXCEEDING_MEAN AS (
	SELECT 
		 [YEAR]
		,[department_id]
		,[DEPARTMENT]
		,DEPARTMENT_HIRES_YEAR
		,MEAN_YEAR
		,IS_EXCEEDING_MEAN_YEAR	= CASE WHEN DEPARTMENT_HIRES_YEAR > MEAN_YEAR THEN 'Yes' ELSE 'No' END
		,IDX					= ROW_NUMBER() OVER (PARTITION BY [YEAR] ORDER BY DEPARTMENT_HIRES_YEAR DESC)
	FROM CTE_YEAR_MEAN
)

SELECT
	 [YEAR]
	,[department_id]
	,[DEPARTMENT]
	,DEPARTMENT_HIRES_YEAR
	,IDX
FROM CTE_DEPARTMENTS_EXCEEDING_MEAN
WHERE IS_EXCEEDING_MEAN_YEAR = 'Yes'
