CREATE EXTERNAL TABLE IF NOT EXISTS cloudfront_logs_parquet (
  `Date` DATE,
  Time STRING,
  Location STRING,
  Bytes INT,
  RequestIP STRING,
  Method STRING,
  Host STRING,
  Uri STRING,
  Status INT,
  Referrer STRING,
  os STRING,
  Browser STRING,
  BrowserVersion STRING
  )
STORED AS PARQUET
LOCATION 's3://my-s3-bucket/athena/cloudfront_logs_parquet/'
tblproperties ("parquet.compress"="GZIP");