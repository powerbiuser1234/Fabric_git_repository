CREATE PROCEDURE CFR.Etl_Cntrl_SP
    @BatchKey SMALLINT,
    @TaskName VARCHAR(100),
    @BatchDate DATE,
    @BatchID SMALLINT,
    @Status VARCHAR(50),
    @ErrorInfo VARCHAR(8000) = NULL,
    @Duration TIME(3) = NULL
AS
BEGIN
    --Validation 1: Check if BatchDate and BatchID already exist
    IF @TaskName LIKE 'Pp%' AND EXISTS (
        SELECT 1
        FROM CFR.BatchProcessingLog
        WHERE BatchDate = @BatchDate
          AND BatchID = @BatchID
          AND TaskName = @TaskName
          AND Status = 'Succeeded'
    )
    BEGIN
        RAISERROR('A record with the same BatchDate and BatchID with Succeeded status already exists.', 16, 1);
        RETURN;
    END

    --Validation 2: If Input Status is valid
    IF @Status NOT IN ('Started', 'Succeeded', 'Failed')
    BEGIN
        RAISERROR('Invalid Status. Status must be one of ''Started'', ''Succeeded'', or ''Failed''.', 16, 1);
        RETURN;
    END

    --Validation 3: that the BatchDate is not set to a future date
    IF @BatchDate > GETDATE()
    BEGIN
        RAISERROR('BatchDate cannot be in the future.', 16, 1);
        RETURN;
    END

    --Detect TaskType based on TaskName
    DECLARE @TaskType VARCHAR(100);

    IF @TaskName LIKE 'Pp%' OR @TaskName LIKE 'P%'
        SET @TaskType = 'Pipeline';
    ELSE IF @TaskName LIKE 'Nb%'
        SET @TaskType = 'Notebook';
    ELSE IF @TaskName LIKE 'Cd%'
        SET @TaskType = 'CopyData';
    ELSE
        SET @TaskType = NULL;

    --Set ExecutionTime as current datetime
    DECLARE @ExecutionTime DATETIME2(3) = CURRENT_TIMESTAMP;

    IF @Status <> 'Started'
    BEGIN
        SELECT TOP 1 @Duration = CAST(DATEADD(SECOND, ABS(DATEDIFF(SECOND, CURRENT_TIMESTAMP, ExecutionTime)), 0) AS TIME)
        FROM CFR.BatchProcessingLog
        WHERE TaskName = @TaskName 
        AND BatchID = @BatchID 
        AND Status = 'Started'
        ORDER BY ExecutionTime DESC;
    END


    --Insert into the table
    INSERT INTO CFR.BatchProcessingLog (
        BatchKey,
        BatchID,
        TaskType,
        TaskName,
        BatchDate,
        Status,
        ErrorInfo,
        ExecutionTime,
        Duration
    )
    VALUES (
        @BatchKey,
        @BatchID,
        @TaskType,
        @TaskName,
        @BatchDate,
        @Status,
        @ErrorInfo,
        @ExecutionTime,
        @Duration
    );
END;