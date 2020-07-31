CREATE OR REPLACE PROCEDURE STREAM_CONSUME_MSG
(
  region IN VARCHAR2,
  stream_id IN VARCHAR2,
  credential_name IN VARCHAR2
) AS
  /* Oracle Streaming Service (OSS) cursor variables */
  oss_cursor_response dbms_cloud_TYPES.resp;
  oss_cursor_request VARCHAR2(500);
  oss_cursor_element JSON_ELEMENT_T;
  oss_cursor_object JSON_OBJECT_T;
  oss_cursor_value VARCHAR2(500);
  cursor_json VARCHAR2(500);
  /* OSS retrieved message variables */
  get_messages_response dbms_cloud_TYPES.resp;
  messages_element JSON_ELEMENT_T;
  messages_array  JSON_ARRAY_T;
  messages_object JSON_OBJECT_T;
  messages_header JSON_OBJECT_T;
  message_element JSON_ELEMENT_T;
  message_json CLOB;
  message JSON_OBJECT_T;
  row_value JSON_OBJECT_T;
  transaction_type VARCHAR2(10);
  /* POS transaction variables extracted from the JSON retrieved from the OSS stream   */
  
   r_tran_id       NUMBER(38,0);
   r_created_on    TIMESTAMP(9) :=SYSTIMESTAMP;    --assume message does not contain. 
   r_location      VARCHAR2(26 BYTE);
   r_hour_of_day   NUMBER(38,0);
   r_register_id   NUMBER(10,0);
   r_pos_date      DATE;
   r_pos_id        NUMBER(20,0);
   r_pos_ln_id     NUMBER(10,0);
   r_product_id    VARCHAR2(60 BYTE);
   r_units         NUMBER(10,0);
   r_sales_price   NUMBER(20,2);
   r_retail_price  NUMBER(20,2);
   r_reward_id     VARCHAR2(26 BYTE);
   r_promo_id      VARCHAR2(26 BYTE);
   r_coupon_id     VARCHAR2(26 BYTE);
   r_zip_code      VARCHAR2(26 BYTE);         
   r_fips          VARCHAR2(26 BYTE);  

 /* End of OSS Stream Variables */

  last_offset NUMBER(18,0);
  max_offset  NUMBER(18,0);
  err_code    NUMBER(18,0);
  err_msg     VARCHAR2(512);
BEGIN
    dbms_output.put_line('1'); 
  
      SELECT COALESCE( MAX(last_offset), 0 ) INTO last_offset FROM STREAM_CONSUME_OFFSET_POSITION;
   		 IF( last_offset = 0 ) THEN
    		
             oss_cursor_request := JSON_OBJECT(
                            'partition' value 0,
                            'type' value 'TRIM_HORIZON'
                        );
  			 INSERT INTO STREAM_CONSUME_OFFSET_POSITION (last_offset) VALUES (0);
  
  		ELSE
           oss_cursor_request := JSON_OBJECT(
                                'partition' value 0,
                                'type' value 'AFTER_OFFSET',
                                'offset' value last_offset                        
                        );
    	END IF;
          
  
    dbms_output.put_line('2 - Cursor offset: ' || last_offset); 
  
    utl_http.close_persistent_conns();
    
    /*
        make the REST API call to get a cursor
    */
   
    oss_cursor_response := DBMS_CLOUD.SEND_REQUEST(
            credential_name => credential_name,
            uri => 'https://streaming.' || region || '.oci.oraclecloud.com/20180418/streams/' || stream_id || '/cursors',
            method => DBMS_CLOUD.METHOD_POST,
            body => UTL_RAW.CAST_TO_RAW(oss_cursor_request)
          );   
    /*
        parse the JSON string into an object
    */
  
    oss_cursor_element := JSON_ELEMENT_T.PARSE( dbms_cloud.get_response_text(oss_cursor_response) );
    IF (oss_cursor_element.is_Object) THEN
        oss_cursor_object := treat(oss_cursor_element AS JSON_OBJECT_T);
        oss_cursor_value := oss_cursor_object.get_String('value');
        --dbms_output.put_line(oss_cursor_value);
    END IF;
   
    cursor_json := utl_raw.cast_to_varchar2(
                                utl_encode.base64_decode(
                                utl_raw.cast_to_raw(
                                oss_cursor_object.get_String('value')
                                )
                         )
                );     
  

  /** PRINT CURSOR DETAILS **/   
    -- dbms_output.put_line(cursor_json);
  
/** GET STREAMED MESSAGES **/    
    get_messages_response := DBMS_CLOUD.SEND_REQUEST(
            credential_name => credential_name,
            uri => 'https://streaming.' || region || '.oci.oraclecloud.com/20180418/streams/' || stream_id || '/messages?cursor=' || oss_cursor_value,
            method => dbms_cloud.METHOD_GET
          );
  
    
 --   dbms_output.put_line(dbms_cloud.get_response_text(get_messages_response));
 --   dbms_output.put_line(dbms_cloud.get_response_headers(oss_cursor_response).to_clob);
 --   dbms_output.put_line(dbms_cloud.get_response_status_code(oss_cursor_response));  
    
   
   /**MESSAGES FROM get_message_responses **/   
  
    messages_element := JSON_ELEMENT_T.parse( dbms_cloud.get_response_text(get_messages_response));
  
    IF (messages_element.is_Array) THEN
        messages_array := treat(messages_element AS JSON_ARRAY_T);
        
        dbms_output.put_line('Messages - ' || messages_array.get_size); 
      
        /*
            loop over message array
        */
            
        FOR i IN 0 .. messages_array.get_size - 1 LOOP
            BEGIN
  
  dbms_output.put_line('Loop' || i);
              
                messages_object := JSON_OBJECT_T(messages_array.get(i));
                max_offset := messages_object.get_Number('offset'); 
  
  dbms_output.put_line('offset -' || max_offset); 
              
              
                message_json := utl_raw.cast_to_varchar2(
                                    utl_encode.base64_decode(
                                        utl_raw.cast_to_raw(
                                            messages_object.get_String('value')
                                        )
                                    )
                                );
                
                message_element := JSON_ELEMENT_T.parse(message_json);
                            
              
              
                IF( message_element.is_Object ) THEN
                    message := treat(message_element as JSON_OBJECT_T);
                    row_value := message.get_Object('value');
                END IF;
                transaction_type := message.get_String('type');
                /*
                    now grab the user information from the JSON
                    object in this message
                */
                r_tran_id       := row_value.get_Number('tran_id');
                r_created_on    := coalesce( row_value.get_Date('created_on'), sysdate );
                r_location      := row_value.get_String('location');
                r_hour_of_day   := row_value.get_Number('hour_of_day');
                r_register_id   := row_value.get_Number('register_id');
                r_pos_date      := coalesce( row_value.get_Date('pos_date'), sysdate );
                r_pos_id        := row_value.get_Number('pos_id');
                r_pos_ln_id     := row_value.get_Number('pos_ln_id');
                r_product_id    := row_value.get_Number('product_id');
                r_units         := row_value.get_Number('units');
                r_sales_price   := row_value.get_Number('sales_price');
                r_retail_price  := row_value.get_Number('retail_price');
                r_reward_id     := row_value.get_String('reward_id');
                r_promo_id      := row_value.get_String('promo_id');
                r_coupon_id     := row_value.get_String('coupon_id');
                r_zip_code      := row_value.get_Number('zip_code');         
                r_fips          := row_value.get_STRING('fips');  
              
    dbms_output.put_line(r_tran_id ||'-'|| r_created_on || '-' || r_location || '-' || r_hour_of_day);   
              
                IF( transaction_type = 'INSERT' ) THEN
    dbms_output.put_line('insert');
                    INSERT INTO  STREAM_POS_DATA(TRAN_ID, CREATED_ON, LOCATION,HOUR_OF_DAY, REGISTER_ID, POS_DATE,POS_ID, POS_LN_ID, PRODUCT_ID, UNITS,                                  SALES_PRICE, RETAIL_PRICE, REWARD_ID,PROMO_ID, COUPON_ID, ZIP_CODE, FIPS)          
                    VALUES (
                    r_tran_id,   
                    r_created_on, 
                	r_location, 
                	r_hour_of_day,   
                	r_register_id,
                    r_pos_date,  
                	r_pos_id,     
                	r_pos_ln_id,   
                    r_product_id,
                	r_units,    
                	r_sales_price,   
                	r_retail_price,  
                    r_reward_id,
                    r_promo_id,  
                    r_coupon_id,     
                    r_zip_code,          
                    r_fips );
                END IF;
                EXCEPTION
                WHEN OTHERS
                THEN
                    -- INSERT INTO ERROR_LOGGING (ERROR_OFFSET_ID,ERROR_MESSAGE) VALUES (max_offset, message_json );
                    err_code := SQLCODE;
                    err_msg := SUBSTR(SQLERRM,1,500);
                    dbms_output.put_line(message_json);
                    dbms_output.put_line(err_msg);
                    CONTINUE;
            END;        
        END LOOP;
    END IF;
    IF( COALESCE( max_offset, 0) > last_offset ) THEN
        UPDATE STREAM_CONSUME_OFFSET_POSITION set last_offset = max_offset;
    END IF;
    /* END GET MESSAGES */
EXCEPTION
    WHEN OTHERS
    THEN
        /**Maybe a Streaming Log Table?? **/
        dbms_output.put_line(SQLERRM);   
        IF( COALESCE( max_offset, 0) > last_offset ) THEN
            UPDATE STREAM_CONSUME_OFFSET_POSITION set last_offset = max_offset;
        END IF;
END STREAM_CONSUME_MSG;