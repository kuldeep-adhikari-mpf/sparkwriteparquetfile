# sparkwriteparquetfile
#Send spark job to gcloud for creating parquet file in data proc cluster
 gcloud dataproc jobs submit spark --cluster=dataproc-hdp-cluster1-qa --region us-east1    --class sql.SparkSQLExample --jars target/scala-2.11/sparkwriteparquetfile_2.11-0.1.jar
