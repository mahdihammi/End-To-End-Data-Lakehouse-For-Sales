SELECT 
    * 
FROM 
    read_parquet('s3://lakehouse-project/lakehouse-raw/sales/load_date=*/*.parquet')