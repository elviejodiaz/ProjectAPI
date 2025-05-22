CREATE VIEW [dbo].[VW_EMPLOYEES_HIRED_PER_DEPARTMENT_JOB_YEAR_QUARTER]
AS
------------
--Description: View that returns the number of employees hired per department, job, year and quarter
--			   This is to solve first requirement on Challenge #2
--No	Date		Description
--01	2025-05-22	View creation
------------
WITH CTE AS (
	SELECT 
		 B.department
		,C.job
		,D.[Year]
		,Q1				= SUM(CASE WHEN D.QuarterName = 'Q1' THEN 1 ELSE 0 END)
		,Q2				= SUM(CASE WHEN D.QuarterName = 'Q2' THEN 1 ELSE 0 END)
		,Q3				= SUM(CASE WHEN D.QuarterName = 'Q3' THEN 1 ELSE 0 END)
		,Q4				= SUM(CASE WHEN D.QuarterName = 'Q4' THEN 1 ELSE 0 END)
	FROM dbo.HiredEmployees A
		INNER JOIN dbo.Departments B
			ON A.department_id = B.id
		INNER JOIN dbo.Jobs C
			ON A.job_id = C.id
		INNER JOIN dbo.DimDate D
			ON CAST(A.[datetime] AS DATE) = D.FullDate
	--WHERE D.[Year] = 2021
	GROUP BY
		 B.department
		,C.job
		,D.[Year]
)

, CTE_ORDER AS (
	SELECT
		 department
		,job
		,[Year]
		,Q1
		,Q2
		,Q3
		,Q4
		,IDX		= ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY department, job, [Year])
	FROM CTE
)
	
SELECT 
	 department
	,job
	,[Year]
	,Q1
	,Q2
	,Q3
	,Q4
	,IDX
FROM CTE_ORDER

