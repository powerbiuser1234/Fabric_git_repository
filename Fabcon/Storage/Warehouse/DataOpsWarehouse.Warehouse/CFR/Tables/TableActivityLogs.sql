CREATE TABLE [CFR].[TableActivityLogs] (

	[BatchID] smallint NULL, 
	[BatchDate] date NULL, 
	[SourceSystem] varchar(100) NOT NULL, 
	[ActivityType] varchar(100) NULL, 
	[ItemName] varchar(200) NULL, 
	[Counts] bigint NULL, 
	[Status] varchar(50) NULL, 
	[Message] varchar(600) NULL, 
	[RunStart] datetime2(3) NULL, 
	[RunEnd] datetime2(3) NULL, 
	[ExecDurationSec] int NULL
);