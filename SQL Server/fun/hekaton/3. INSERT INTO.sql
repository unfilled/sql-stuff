USE test;
GO

DECLARE @src AS table (
    uuid        uniqueidentifier PRIMARY KEY DEFAULT NEWID(),
    task_id     bigint,
    stage       int,
    valid_to    datetime2(0) DEFAULT CURRENT_TIMESTAMP
);

; WITH n AS (SELECT 0 AS n UNION ALL SELECT 0 AS n)
, n1 AS (SELECT 0 AS n FROM n n1, n n2)
, n2 AS (SELECT 0 AS n FROM n1 n1, n1 n2)
, n3 AS (SELECT 0 AS n FROM n2 n1, n2 n2)
INSERT INTO @src (task_id, stage)
SELECT 
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS task_id,
    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) % 10 AS stage
FROM n3, n2;

SET STATISTICS TIME, IO ON;

INSERT INTO diskTbl WITH (TABLOCKX) (uuid, task_id, stage, valid_to)
SELECT uuid, task_id, stage, valid_to
FROM @src;

INSERT INTO schemaOnlyTbl (uuid, task_id, stage, valid_to)
SELECT uuid, task_id, stage, valid_to
FROM @src;

INSERT INTO durableTbl(uuid, task_id, stage, valid_to)
SELECT uuid, task_id, stage, valid_to
FROM @src;

SET STATISTICS TIME, IO OFF;