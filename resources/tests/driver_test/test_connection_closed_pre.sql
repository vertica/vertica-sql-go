-- Remove user if needed.
DROP USER IF EXISTS TestGuy;

CREATE USER TestGuy IDENTIFIED BY 'TestGuyPass';
GRANT USAGE ON SCHEMA PUBLIC to TestGuy;