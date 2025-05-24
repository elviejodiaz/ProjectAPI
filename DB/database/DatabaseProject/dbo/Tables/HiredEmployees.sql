CREATE TABLE [dbo].[HiredEmployees] (
    [id] INT NOT NULL,
    [name] NVARCHAR(4000) NULL,
    [datetime] NVARCHAR(4000) NULL,
    [department_id] INT NULL,
    [job_id] INT NULL,
    FOREIGN KEY (department_id) REFERENCES dbo.Departments(id),
    FOREIGN KEY (job_id) REFERENCES dbo.Jobs(id)
);



GO

