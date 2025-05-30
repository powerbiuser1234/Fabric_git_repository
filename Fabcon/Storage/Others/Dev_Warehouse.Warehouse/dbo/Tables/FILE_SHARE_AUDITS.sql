CREATE TABLE [dbo].[FILE_SHARE_AUDITS] (

	[Request_ID] varchar(255) NULL, 
	[Requester_Email] varchar(255) NULL, 
	[Requester_Dept] varchar(100) NULL, 
	[Requirement_details] varchar(max) NULL, 
	[Requirement_documents] varchar(max) NULL, 
	[Data_custodian] varchar(255) NULL, 
	[File_shared] varchar(255) NULL, 
	[Approver_Email] varchar(255) NULL, 
	[Approver_comment] varchar(max) NULL, 
	[Submitted_time] datetime2(0) NULL, 
	[Provisioning_time] datetime2(0) NULL, 
	[Approval_time] datetime2(0) NULL, 
	[Status] varchar(50) NULL
);