SET SERVEROUTPUT ON
DECLARE
BEGIN
  PublishSalesMessages(
    'us-ashburn-1',
   'ocid1.stream.oc1.iad.amaaaaaakmobi5iacd4vg7lplbg4jsqbphuex4b75hzhzc4trz4zro42krrq',
    'OCI_KEY_CRED'
  );
  
   STREAM_CONSUME_MSG(
    'us-ashburn-1',
    'ocid1.stream.oc1.iad.amaaaaaakmobi5iacd4vg7lplbg4jsqbphuex4b75hzhzc4trz4zro42krrq',
    'OCI_KEY_CRED'
  );
  
END;