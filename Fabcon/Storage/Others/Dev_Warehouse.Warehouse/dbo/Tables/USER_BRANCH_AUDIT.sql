CREATE TABLE [dbo].[USER_BRANCH_AUDIT] (

	[Username] varchar(100) NOT NULL, 
	[Branch_Code] varchar(100) NOT NULL, 
	[Role_Name] varchar(100) NOT NULL, 
	[Status] varchar(20) NOT NULL, 
	[Submitted_Time] datetime2(0) NOT NULL, 
	[Updated_Time] datetime2(0) NULL, 
	[Approver_Email] varchar(100) NULL, 
	[Approver_Comment] varchar(500) NULL
);