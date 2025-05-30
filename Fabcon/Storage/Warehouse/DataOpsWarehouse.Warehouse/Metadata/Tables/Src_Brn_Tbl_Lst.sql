CREATE TABLE [Metadata].[Src_Brn_Tbl_Lst] (

	[ID] int NULL, 
	[SourceSystem] varchar(50) NULL, 
	[SourceSchema] varchar(50) NULL, 
	[SourceTable] varchar(100) NULL, 
	[SourceColumnList] varchar(max) NULL, 
	[TargetSchema] varchar(50) NULL, 
	[TargetTable] varchar(100) NULL, 
	[LoadType] varchar(20) NULL, 
	[FilterCondition] varchar(max) NULL, 
	[PartitionColumn] varchar(50) NULL, 
	[IsActive] bit NULL, 
	[Priority] int NULL, 
	[Comments] varchar(max) NULL, 
	[CreatedDate] datetime2(0) NULL, 
	[ModifiedDate] datetime2(0) NULL
);