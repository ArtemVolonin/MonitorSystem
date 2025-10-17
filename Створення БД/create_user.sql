-- Create the user 
create user RetailGroup
  identified by "1"
  default tablespace USERS
  temporary tablespace TEMP
  profile DEFAULT
  quota unlimited on users;
-- Grant/Revoke role privileges 
grant connect to RetailGroup;
grant resource to RetailGroup;

-----------------------------------
