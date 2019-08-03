-- Create authentication hash.
CREATE AUTHENTICATION v_hash METHOD 'hash' HOST '10.0.0.0/0';

-- Remove user if needed.
DROP USER IF EXISTS TestGuy;

-- Create a new user.
CREATE USER TestGuy IDENTIFIED BY 'TestGuyPassBad';

-- Alter him to require scheme.
ALTER USER TestGuy SECURITY_ALGORITHM '%v' IDENTIFIED BY 'TestGuyPass';

-- Grant user some access.
GRANT USAGE ON SCHEMA PUBLIC to TestGuy;

-- Grant authentication to user.
GRANT AUTHENTICATION v_hash to TestGuy;