CREATE TABLE [CFR].[BatchProcessingLog_Bkp] (

	[ID] smallint NULL, 
	[BatchID] smallint NULL, 
	[TaskType] varchar(100) NULL, 
	[TaskName] varchar(100) NULL, 
	[BatchDate] date NULL, 
	[Status] varchar(50) NULL, 
	[ErrorInfo] varchar(8000) NULL, 
	[ExecutionTime] datetime2(3) NULL, 
	[Duration] time(3) NULL
);