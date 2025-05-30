CREATE PROCEDURE [dbo].[SP_File_Share]
    @ActionType VARCHAR(20), -- 'submit', 'upload', 'approve'
    @Request_ID VARCHAR(255),
    @Requester_Email VARCHAR(255) = NULL,
    @Requester_Dept VARCHAR(100) = NULL,
    @Requirement_details VARCHAR(MAX) = NULL,
    @Requirement_documents VARCHAR(MAX) = NULL,
    @Data_custodian VARCHAR(255) = NULL,
    @File_shared VARCHAR(255) = NULL,
    @Approver_Email VARCHAR(255) = NULL,
    @Approver_comment VARCHAR(MAX) = NULL,
    @Status VARCHAR(50) = NULL
AS
BEGIN
    BEGIN TRY
        IF @ActionType = 'submit'
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM [dbo].[FILE_SHARE_AUDITS]
                WHERE Request_ID = @Request_ID
            )
            BEGIN
                INSERT INTO [dbo].[FILE_SHARE_AUDITS] 
                    (Request_ID, Requester_Email, Requester_Dept, Requirement_details, Requirement_documents, Submitted_time, Status)
                VALUES 
                    (@Request_ID, @Requester_Email, @Requester_Dept, @Requirement_details, @Requirement_documents, GETUTCDATE(), @Status);

                PRINT 'Data submitted successfully';
            END
            ELSE
            BEGIN
                PRINT 'Request_ID already exists. Submission skipped.';
            END
        END

        ELSE IF @ActionType = 'upload'
        BEGIN
            UPDATE [dbo].[FILE_SHARE_AUDITS]
            SET Data_custodian = @Data_custodian,
                File_shared = @File_shared,
                Provisioning_time = GETUTCDATE(),
                Status = @Status
            WHERE Request_ID = @Request_ID;

            PRINT 'File uploaded and provisioned by Data Custodian.';
        END

        ELSE IF @ActionType = 'approve'
        BEGIN
            UPDATE [dbo].[FILE_SHARE_AUDITS]
            SET Approver_Email = @Approver_Email,
                Approver_comment = @Approver_comment,
                Approval_time = GETUTCDATE(),
                Status = @Status
            WHERE Request_ID = @Request_ID;

            PRINT 'Request approved.';
        END

        ELSE
        BEGIN
            PRINT 'Invalid ActionType provided.';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;