CREATE TABLE [STAGE].[LOADERRORS] (
    [INGESTION_DATETIME] DATETIME2 NOT NULL,
    [TABLE_NAME] NVARCHAR(200) NOT NULL,
    [RECORD] NVARCHAR(MAX) NOT NULL
);

GO