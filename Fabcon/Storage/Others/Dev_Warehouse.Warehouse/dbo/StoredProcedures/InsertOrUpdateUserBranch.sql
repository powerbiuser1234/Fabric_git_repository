CREATE PROCEDURE [dbo].[InsertOrUpdateUserBranch]
    @Username VARCHAR(255),
    @Branch_Code VARCHAR(50),
    @Role_Name VARCHAR(255),
    @Status VARCHAR(50) = 'Pending' -- Default status is 'Pending'
AS
BEGIN
    BEGIN TRY
        -- Check if the record already exists
        IF EXISTS (
            SELECT 1 FROM [dbo].[user_branch_new]
            WHERE Username = @Username AND Branch_Code = @Branch_Code AND Role_Name = @Role_Name
        )
        BEGIN
            -- Update the status only if it's changed
            UPDATE [dbo].[user_branch_new]
            SET Status = @Status
            WHERE Username = @Username AND Branch_Code = @Branch_Code AND Role_Name = @Role_Name;

            PRINT 'Status updated successfully';
        END
        ELSE
        BEGIN
            -- Insert a new record with 'Pending' status
            INSERT INTO [dbo].[user_branch_new] (Username, Branch_Code, Role_Name, Status)
            VALUES (@Username, @Branch_Code, @Role_Name, @Status);

            PRINT 'Data inserted successfully';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred: ' + ERROR_MESSAGE();
    END CATCH
END;