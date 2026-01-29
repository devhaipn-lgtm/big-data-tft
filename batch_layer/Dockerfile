# Use the Bitnami Spark image as it includes most environment needs
FROM bitnami/spark:3.3.2

USER root

# Install Python MongoDB and PySpark dependencies
RUN pip install pymongo findspark

# Download the MongoDB Spark Connector JARs so Spark can talk to Mongo
# These are required for the .format("mongo") call in your script
ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar /opt/bitnami/spark/jars/
ADD https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.10/mongo-java-driver-3.12.10.jar /opt/bitnami/spark/jars/

# Set workdir
WORKDIR /processing

# The spark_processor.py is usually mounted via volume in docker-compose, 
# but we can copy it as a fallback
COPY spark_processor.py .

# No CMD needed here as this is typically run via docker exec