CREATE EXTERNAL TABLE IF NOT EXISTS elb_logs_raw_native_parquet (
  request_timestamp string, 
  elb_name string, 
  request_ip string, 
  request_port int, 
  backend_ip string, 
  backend_port int, 
  request_processing_time double, 
  backend_processing_time double, 
  client_response_time double, 
  elb_response_code string, 
  backend_response_code string, 
  received_bytes bigint, 
  sent_bytes bigint, 
  request_verb string, 
  url string, 
  protocol string, 
  user_agent string, 
  ssl_cipher string, 
  ssl_protocol string ) 
STORED AS PARQUET
LOCATION 's3://my-s3-bucket/athena/elb_logs_raw_native/'
tblproperties ("parquet.compress"="GZIP");