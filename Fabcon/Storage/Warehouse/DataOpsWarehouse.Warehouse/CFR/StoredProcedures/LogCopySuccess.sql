-- 2. Create or alter under your CFR schema
CREATE   PROCEDURE CFR.LogCopySuccess
    @BatchID      SMALLINT,
	@BatchDate Date,
    @SourceSystem  VARCHAR(100),
    @ActivityType  VARCHAR(100),
    @ItemName      VARCHAR(200),
	@Counts BIGINT,
	@Status VARCHAR(50),
    @Message   VARCHAR(600),
	@RunStart [datetime2](3),
	@RunEnd [datetime2](3),
	@ExecDurationSec INT
AS
BEGIN
    INSERT INTO CFR.TableActivityLogs
        (BatchID,
		BatchDate,
         SourceSystem,
         ActivityType,
         ItemName,
		 Counts,
         Status,
         Message,
         RunStart,
		 RunEnd,
		 ExecDurationSec
		 )
    VALUES
        (
            @BatchID,
			@BatchDate,
            @SourceSystem,
            @ActivityType,
            @ItemName,
			@Counts,
            @Status,               -- hard-coded status
            @Message,
            -- use GETDATE() or SYSDATETIME(); SQL will cast to datetime2(3)
            @RunStart,
			@RunEnd,
			@ExecDurationSec
        );
END;