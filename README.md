# sparkwriteparquetfile
#Send spark job to gcloud for creating parquet file in data proc cluster
gsutil cp -r target/scala-2.11/sparkwriteparquetfile_2.11-0.1.jar  gs://mp-data-ingestion-v1/test/scala
[kuldeep.adhikari@control-node-1-na-int ~]$ gcloud dataproc jobs submit spark --cluster=dataproc-hdp-cluster1-qa --region us-east1    --class sql.SparkSQLExample --jars gs://mp-data-ingestion-v1/test/scala/sparkwriteparquetfile_2.11-0.1.jar
 #gcloud dataproc jobs submit spark --cluster=dataproc-hdp-cluster1-qa --region us-east1    --class sql.SparkSQLExample --jars target/scala-2.11/sparkwriteparquetfile_2.11-0.1.jar
#protoc --proto_path=src --java_out=src src/main/proto/visitor_export.proto