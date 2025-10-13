CREATE OR REPLACE PROCEDURE create_users_from_emails()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    user_name VARCHAR;
    create_user_sql VARCHAR;
    c1 CURSOR FOR SELECT REPLACE(REPLACE(SPLIT_PART(EMAIL, '@', 1),'.','_'),'-','_') cleanuser FROM SETUP.PUBLIC.TECHUPUSERS;
BEGIN
    FOR record IN c1 DO
        user_name := record.cleanuser;                
        create_user_sql := 'CREATE OR REPLACE USER ' || user_name || ' PASSWORD=''abc123'' DEFAULT_ROLE = ACCOUNTADMIN DEFAULT_SECONDARY_ROLES = (''ALL'') MUST_CHANGE_PASSWORD = TRUE;';        
        EXECUTE IMMEDIATE create_user_sql;
    END FOR;
    
    RETURN 'User creation process completed successfully.';
END;
$$;
---
-- Call the stored procedure to run the script


CALL create_users_from_emails();


CREATE OR REPLACE PROCEDURE grant_role()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    user_name VARCHAR;
    create_user_sql VARCHAR;
    c1 CURSOR FOR SELECT REPLACE(REPLACE(SPLIT_PART(EMAIL, '@', 1),'.','_'),'-','_') cleanuser FROM SETUP.PUBLIC.TECHUPUSERS;
BEGIN
    FOR record IN c1 DO
        user_name := record.cleanuser;                
        create_user_sql := 'GRANT ROLE ACCOUNTADMIN TO USER ' || user_name || ';';        
        EXECUTE IMMEDIATE create_user_sql;
    END FOR;
    
    RETURN 'User creation process completed successfully.';
END;
$$;

CALL grant_role();



