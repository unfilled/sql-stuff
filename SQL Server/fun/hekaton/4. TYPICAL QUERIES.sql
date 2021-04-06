DECLARE @src AS table (
    uuid        uniqueidentifier PRIMARY KEY DEFAULT NEWID(),
    task_id     bigint,
    stage       int,
    valid_to    datetime2(0) DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO @src
SELECT TOP 150 * FROM diskTbl
ORDER BY NEWID();

SET STATISTICS TIME, IO ON;

UPDATE diskTbl
SET stage += 1
WHERE uuid IN (SELECT uuid FROM @src);

UPDATE schemaOnlyTbl
SET stage += 1
WHERE uuid IN (SELECT uuid FROM @src);

UPDATE durableTbl
SET stage += 1
WHERE uuid IN (SELECT uuid FROM @src);

SET STATISTICS TIME, IO OFF;
GO

SET STATISTICS TIME, IO ON;

UPDATE diskTbl
SET valid_to = DATEADD(SECOND, task_id, valid_to);

UPDATE schemaOnlyTbl
SET valid_to = DATEADD(SECOND, task_id, valid_to);

UPDATE durableTbl
SET valid_to = DATEADD(SECOND, task_id, valid_to);

SET STATISTICS TIME, IO OFF;
GO

SET STATISTICS TIME, IO ON;

DELETE FROM diskTbl
WHERE valid_to >= DATEADD (SECOND, 8192, CURRENT_TIMESTAMP);

DELETE FROM schemaOnlyTbl
WHERE valid_to >= DATEADD (SECOND, 8192, CURRENT_TIMESTAMP);

DELETE FROM durableTbl
WHERE valid_to >= DATEADD (SECOND, 8192, CURRENT_TIMESTAMP);

SET STATISTICS TIME, IO OFF;
GO

DROP TABLE IF EXISTS #src;

CREATE TABLE #src (
    uuid        uniqueidentifier PRIMARY KEY DEFAULT NEWID(),
    task_id     bigint,
    stage       int,
    valid_to    datetime2(0) DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO #src
SELECT TOP 75 * FROM diskTbl
ORDER BY NEWID();

--SELECT * FROM @src;

SET STATISTICS TIME, IO ON;

UPDATE diskTbl
SET stage += 1
WHERE uuid IN (SELECT uuid FROM #src);

UPDATE schemaOnlyTbl
SET stage += 1
WHERE uuid IN (SELECT uuid FROM #src);

UPDATE durableTbl
SET stage += 1
WHERE uuid IN (SELECT uuid FROM #src);
GO 50

/*
SELECT *
FROM diskTbl;

SELECT * 
FROM schemaOnlyTbl;

SELECT * 
FROM durableTbl;
*/

SET STATISTICS TIME, IO OFF;