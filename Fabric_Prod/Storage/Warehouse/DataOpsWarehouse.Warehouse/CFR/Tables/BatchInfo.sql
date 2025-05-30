CREATE TABLE [CFR].[BatchInfo] (

	[BatchKey] smallint NULL, 
	[SourceSystem] varchar(100) NOT NULL, 
	[Description] varchar(255) NULL, 
	[CreatedDate] datetime2(3) NULL
);