CREATE PROCEDURE [STAGE].[USP_LOAD_DATA_TO_FINAL_TABLES]
AS
BEGIN
------------
--Description: Procedure updates data on final tables using stage data
--No	Date		Description
--01	2025-05-18	Proc creation
--02	2025-05-19	Added logic to log data load errors
------------

------------
--Sync jobs
------------
INSERT INTO [dbo].[Jobs]
SELECT * FROM [STAGE].[Jobs]
WHERE NULLIF(ID,'') IS NOT NULL

UPDATE [dbo].[Jobs]
SET [JOB] = B.[JOB]
FROM [dbo].[Jobs] A
   INNER JOIN [STAGE].[Jobs] B
      ON A.ID = B.ID
WHERE NULLIF(B.ID,'') IS NOT NULL

--Log load errors Jobs
INSERT INTO [STAGE].[LOADERRORS]
SELECT 
	 INGESTION_TIME = GETDATE()
	,TABLE_NAME		= 'Jobs'
	,RECORD			= CAST(COALESCE(ID,'') AS nvarchar(4000)) + ',' + 
					  CAST(COALESCE(JOB,'') AS nvarchar(4000)) + ','
FROM [STAGE].[Jobs]
WHERE NULLIF(ID,'') IS NULL

------------
--Sync deparments
------------
INSERT INTO [dbo].[Departments]
SELECT * FROM [STAGE].[Departments]
WHERE NULLIF(ID,'') IS NOT NULL

UPDATE [dbo].[Departments]
SET [DEPARTMENT] = B.[DEPARTMENT]
FROM [dbo].[Departments] A
   INNER JOIN [STAGE].[Departments] B
      ON A.ID = B.ID
WHERE NULLIF(B.ID,'') IS NOT NULL

--Log load errors Departments
INSERT INTO [STAGE].[LOADERRORS]
SELECT 
	 INGESTION_TIME = GETDATE()
	,TABLE_NAME		= 'Jobs'
	,RECORD			= CAST(COALESCE(ID,'') AS nvarchar(4000)) + ',' + 
					  CAST(COALESCE(department,'') AS nvarchar(4000)) + ','
FROM [STAGE].[Departments]
WHERE NULLIF(ID,'') IS NULL

------------
--Sync hired employees
------------
INSERT INTO [dbo].[HiredEmployees]
SELECT * FROM [STAGE].[HiredEmployees]
WHERE NULLIF(department_id,'') IS NOT NULL
   AND NULLIF(job_id,'') IS NOT NULL

UPDATE [dbo].[HiredEmployees]
SET [NAME] = B.[NAME],
    [DATETIME] = B.[DATETIME],
	[DEPARTMENT_ID] = B.[DEPARTMENT_ID],
	[JOB_ID] = B.[JOB_ID]
FROM [dbo].[HiredEmployees] A
   INNER JOIN [STAGE].[HiredEmployees] B
      ON A.ID = B.ID
WHERE NULLIF(B.department_id,'') IS NOT NULL
   AND NULLIF(B.job_id,'') IS NOT NULL

--Log load errors HiredEmployees
INSERT INTO [STAGE].[LOADERRORS]
SELECT
	 INGESTION_TIME = GETDATE()
	,TABLE_NAME		= 'HiredEmployees'
	,RECORD			= CAST(COALESCE(ID,'') AS nvarchar(4000)) + ',' + 
					  CAST(COALESCE(NAME,'') AS nvarchar(4000)) + ',' + 
					  CAST(COALESCE(DATETIME,'') AS nvarchar(4000)) + ',' + 
					  CAST(COALESCE(department_id,'') AS nvarchar(4000)) + ',' + 
					  CAST(COALESCE(job_id,'') AS nvarchar(4000))
FROM [STAGE].[HiredEmployees]
WHERE NULLIF(ID,'') IS NULL
	OR NULLIF(department_id,'') IS NULL
	OR NULLIF(job_id,'') IS NULL

END