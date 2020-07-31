create or replace procedure PublishSalesMessages
(
  region IN VARCHAR2,
  stream_id IN VARCHAR2,
  credential_name IN VARCHAR2
) AS
  /* Oracle Streaming Service (OSS) cursor variables */

 
  /* OSS retrieved message variables */
  get_messages_response dbms_cloud_TYPES.resp;
  transaction_type VARCHAR2(10);
  message_details_json VARCHAR2(2000);
  response DBMS_CLOUD_TYPES.resp;
  response_text VARCHAR2(4000);
  error_message VARCHAR2(8000);

  /*
    User specific variables extracted from the JSON
    retrieved from the OSS stream
  */
 
  r_pos  pos_data_client%rowtype;
  r_sales_price Number(10,2);

--receipt_ID, zip_code, location, pos_id, category, subcategory, quantity, sales, pos_date, hour_of_day, retail_price, FIPS, register_id
  
  cursor c_pos_data
  IS
    select *   
    from
       pos_data_client
    where rownum < 10
    order by hour_of_day;

 BEGIN
    
    
  FOR r_pos IN c_pos_data
  	LOOP
        -- data mapping ...handle returns
        IF r_pos.quantity <> 0 then r_sales_price := r_pos.sales / r_pos.quantity;
        ELSE r_sales_price := 0;
        END IF;  
        
    	-- dbms_output.put_line( r_pos.receipt_ID || ' : ' ||  r_pos.location  || ' : ' || r_pos.category );
        transaction_type := 'INSERT';
        message_details_json := json_object(
                    'messages' value json_array(
                        json_object(
                            'key' value null,
                            'value' value replace (replace (utl_raw.cast_to_varchar2(
                                utl_encode.base64_encode(
                                    utl_raw.cast_to_raw(
                                            json_object(
                                            'type' value transaction_type,
                                            'value'           value json_object(
                                               'tran_id'      value r_pos.receipt_ID, 
                                               'created_on'   value null,
                                               'location'     value r_pos.location, 
                                               'hour_of_day'  value r_pos.hour_of_day,
    										   'register_id'  value r_pos.register_id, 
                                               'pos_date'     value r_pos.pos_date,
                                               'pos_id'       value r_pos.pos_id,
                                               'pos_ln_id'    value r_pos.pos_ln_id,
                                               'product_id'   value r_pos.product_id,
                                               'units'        value r_pos.quantity, 
                                               'sales_price'  value r_sales_price,
                                               'retail_price' value r_pos.retail_price,
                                               'reward_id'    value r_pos.reward_id, 
                                               'promo_id'     value r_pos.promo_id,
                                               'coupon_id'    value r_pos.coupon_id,
                                               'zip_code'     value r_pos.zip_code,
                                               'fips'         value r_pos.fips                               
                                            )
                                        )
                                    )
                                )
                            ), chr (13), ''), chr (10), '')   
                        )
                    )
                );
   
   response := DBMS_CLOUD.send_request(
            credential_name => 'OCI_KEY_CRED',
            uri => 'https://streaming.' || region || '.oci.oraclecloud.com/20180418/streams/' || stream_id || '/messages',
            method => DBMS_CLOUD.METHOD_POST,
            body => utl_raw.cast_to_raw(message_details_json)
          );
    UTL_HTTP.CLOSE_PERSISTENT_CONNS();     
    response_text := DBMS_CLOUD.get_response_text(response);     
   
   -- dbms_output.put_line(message_details_json);
   dbms_output.put_line(response_text);
   
   END LOOP;
 
  EXCEPTION
    WHEN OTHERS
    THEN
      dbms_output.put_line('EXCEPTION - 1'); 
      dbms_output.put_line(SQLERRM);
     
 END PublishSalesMessages;