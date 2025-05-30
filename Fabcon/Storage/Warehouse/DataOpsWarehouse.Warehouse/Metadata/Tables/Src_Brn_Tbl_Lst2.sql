CREATE TABLE [Metadata].[Src_Brn_Tbl_Lst2] (

	[ID] int NULL, 
	[SourceSystem] varchar(50) NULL, 
	[SchemaName] varchar(50) NULL, 
	[TableName] varchar(100) NULL, 
	[ColumnName] varchar(max) NULL, 
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