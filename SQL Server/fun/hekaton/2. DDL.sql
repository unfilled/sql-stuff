USE test;
GO

CREATE TABLE diskTbl (
    uuid        uniqueidentifier PRIMARY KEY DEFAULT NEWID(),
    task_id     bigint,
    stage       int,
    valid_to    datetime2(0) DEFAULT CURRENT_TIMESTAMP
);
GO

CREATE NONCLUSTERED INDEX ix_task_id ON diskTbl (task_id);
GO

CREATE NONCLUSTERED INDEX ix_valid_to ON diskTbl(valid_to);
GO

CREATE TABLE schemaOnlyTbl (
    uuid        uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 10240),        
    task_id     bigint NOT NULL,
    stage       int NOT NULL,
    valid_to    datetime2(0) DEFAULT CURRENT_TIMESTAMP,

    INDEX ix_task_id    NONCLUSTERED HASH (task_id) WITH (BUCKET_COUNT = 10240),
    INDEX ix_valid_to   NONCLUSTERED (valid_to)
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);
GO

CREATE TABLE durableTbl (
    uuid        uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED HASH WITH (BUCKET_COUNT = 10240),
    task_id     bigint,
    stage       int,
    valid_to    datetime2(0),

    INDEX ix_task_id    NONCLUSTERED HASH (task_id) WITH (BUCKET_COUNT = 10240),
    INDEX ix_valid_to   NONCLUSTERED (valid_to)
)
WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);
GO