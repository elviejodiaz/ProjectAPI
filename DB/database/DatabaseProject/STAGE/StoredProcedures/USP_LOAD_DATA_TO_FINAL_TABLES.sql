CREATE PROCEDURE [STAGE].[USP_LOAD_DATA_TO_FINAL_TABLES]
AS
BEGIN
------------
--Procedure updates data on final tables using stage data
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

END

GO

