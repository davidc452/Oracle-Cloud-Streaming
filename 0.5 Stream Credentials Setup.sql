SET SERVEROUTPUT ON
DECLARE
BEGIN
 
DBMS_CLOUD.DROP_CREDENTIAL (
   credential_name   => 'OCI_KEY_CRED');
  
DBMS_CLOUD.CREATE_CREDENTIAL (
       credential_name => 'OCI_KEY_CRED',
       user_ocid       => 'ocid1.user.oc1..aaaaaaaavnq',
       tenancy_ocid    => 'ocid1.tenancy.oc1..aaaaaaaa74..hndqrcb4q',
       private_key     => 'MIIEpAIBAAKC...
xf7G9bSmPO1nFsLgPBMWZFmzR6tJxszm1G4owlh9ggvyrCAvt0tbZw==',
       fingerprint     => 'da:...c'
);

END;
/