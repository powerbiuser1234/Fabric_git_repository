CREATE TABLE [Metadata].[Brn_Tbl_Lst_With_Trim] (

	[ID] int NULL, 
	[SourceSystem] varchar(50) NULL, 
	[SourceSchema] varchar(50) NULL, 
	[SourceTable] varchar(100) NULL, 
	[SourceColumnList] varchar(max) NULL, 
	[TargetSchema] varchar(50) NULL, 
	[TargetTable] varchar(100) NULL, 
	[LoadType] varchar(20) NULL, 
	[FilterCondition] varchar(max) NULL, 
	[IsActive] bit NULL, 
	[CreatedDate] datetime2(0) NULL
);