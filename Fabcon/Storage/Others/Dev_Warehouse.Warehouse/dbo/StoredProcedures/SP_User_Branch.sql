CREATE PROCEDURE [dbo].[SP_User_Branch]
    @Username VARCHAR(255),
    @Branch_Code VARCHAR(50),
    @Role_Name VARCHAR(255),
    @Status VARCHAR(50) = 'Pending',-- Default status is 'Pending'
    @Approver_Email VARCHAR(100),
    @Approver_Comment VARCHAR(500)
AS
BEGIN
    BEGIN TRY
        -- Check if the record already exists
        IF EXISTS (
            SELECT 1 FROM [dbo].[USER_BRANCH_AUDIT]
            WHERE Username = @Username AND Branch_Code = @Branch_Code AND Role_Name = @Role_Name
        )
        BEGIN
            -- Update the status and Updated_Time
            UPDATE [dbo].[USER_BRANCH_AUDIT]
            SET Status = @Status,
                Updated_Time = GETUTCDATE(),
                Approver_Email= @Approver_Email,
                Approver_Comment=@Approver_Comment
            WHERE Username = @Username AND Branch_Code = @Branch_Code AND Role_Name = @Role_Name;

            PRINT 'Status updated successfully';
        END
        ELSE
        BEGIN
            -- Insert a new record with Submitted_Time
            INSERT INTO [dbo].[USER_BRANCH_AUDIT] 
                (Username, Branch_Code, Role_Name, Status, Submitted_Time)
            VALUES 
                (@Username, @Branch_Code, @Role_Name, @Status, GETUTCDATE());

            PRINT 'Data inserted successfully';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;