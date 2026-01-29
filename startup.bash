docker cp spark_processor.py bigdataenv-spark-master-1:/tmp/spark_processor.py;

docker exec -it bigdataenv-spark-master-1 /spark/bin/spark-submit `
  --conf spark.pyspark.python=python3 `
  --conf spark.pyspark.driver.python=python3 `
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 `
  --master spark://bigdataenv-spark-master-1:7077 `
  /tmp/spark_processor.py;
  
 docker cp kafka_to_hdfs.py bigdataenv-spark-master-1:/tmp/3_kafka_to_hdfs.py;
 
 docker exec -u 0 -it bigdataenv-spark-master-1 pip3 install kafka-python-ng hdfs;
 
 docker exec -u 0 -it bigdataenv-spark-master-1 pip3 install importlib-metadata;
 
 docker exec -it bigdataenv-spark-master-1 python3 /tmp/3_kafka_to_hdfs.py;