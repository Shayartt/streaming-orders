from .main_loader import MainStreamLoader
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import uuid

# Kafka Dependencies : 
from confluent_kafka.schema_registry import SchemaRegistryClient
import ssl 

# PySpark Dependencies : 
import pyspark.sql.functions as fn
from pyspark.sql.types import StringType 
from pyspark.sql.avro.functions import from_avro

# Second party dependencies :
from utils import get_secret_var


class KafkaStreamLoader(MainStreamLoader):
    """
    Class responsible for reading Kafka Stream, prepare the data and ingest it into delta lake.
    
    We'll make it as dynamic as possible to use it for any streaming tunnel as long as they're coming from same provider (Confluent Kafka) and written into Delta Lake.
    """
    def __init__(self, topic_name: str, deltaTablePath: str, checkpointLocation: str):
        """
        Initialize the KafkaStreamLoader
        """
        self.topic_name = topic_name
        self.deltaTablePath = deltaTablePath
        self.checkpointLocation = checkpointLocation
        
        # Initialize the spark session :
        self.spark = SparkSession.builder.appName("KafkaStreamLoader").getOrCreate()
        
        # Prepare Configuration Variables : 
        self._confluentClusterName = "Lab"
        self.__confluentBootstrapServers = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
        self._confluentTopicName = topic_name
        self.__schemaRegistry = "https://psrc-yorrp.us-east-2.aws.confluent.cloud"
        self.__confluentApiKey = get_secret_var(self.spark, "my-secrets-scope", "confluentApiKey")
        self.__confluentSecret = get_secret_var(self.spark, "my-secrets-scope", "confluentSecret")
        self.__schemaAPIKey = get_secret_var(self.spark, "my-secrets-scope", "schemaAPIKey")
        self.__schemaSecret = get_secret_var(self.spark, "my-secrets-scope", "schemaSecret")
        self._deltaTablePath = deltaTablePath
        self._checkpointsPath = checkpointLocation
        
        # Register Schema : 
        schema_registry_conf = {
            "url" : self.__schemaRegistry,
            'basic.auth.user.info' : '{}:{}'.format(self.__schemaAPIKey, self.__schemaSecret)
        }

        self.__schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Init Streaming Obj : 
        self.streamingDf = self.read_stream()
        
    def __str__(self) -> str:
        return (f"KafkaStreaming tunnel for topic {self.topic_name} is ready to be used. \n Delta Table Path : {self.deltaTablePath} \n Checkpoint Location : {self.checkpointLocation} \n Schema Key : {self.__schemaAPIKey} \n Schema Secret : {self.__schemaSecret} \n Confluent Key : {self.__confluentApiKey} \n Confluent Secret : {self.__confluentSecret} \n Confluent Cluster Name : {self._confluentClusterName} \n Confluent Bootstrap Servers : {self.__confluentBootstrapServers} \n Confluent Topic Name : {self._confluentTopicName} \n Schema Registry : {self.__schemaRegistry}")
        
        
    def read_stream(self) -> SparkDataFrame:
        """
        Read bytes streaming from kafka and transform it into a Spark Streaming dataframe.
        
        :param offset_stream: str
        
        :return: SparkDataFrame
        """
        binary_to_string = fn.udf(lambda x: str(int.from_bytes(x, byteorder='big')), StringType())
        return (
                self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.__confluentBootstrapServers)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(self.__confluentApiKey, self.__confluentSecret))
                .option("kafka.ssl.endpoint.identification.algorithm", "https")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("subscribe", self._confluentTopicName)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .load()
                .withColumn('key', fn.col("key").cast(StringType()))
                .withColumn('fixedValue', fn.expr("substring(value, 6, length(value)-5)"))
                .withColumn('valueSchemaId', binary_to_string(fn.expr("substring(value, 2, 4)")))
                .select('topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', 'valueSchemaId','fixedValue')
        )
        
    def parse_stream_data(self, df, ephoch_id) -> None:
        """
        Function used to parse stream data into readable format (from avro).
        
        This function will load from the schema registry the latest schema and parse the data correctly, finally it's will save it into delta lake.
        
        :param df: SparkDataFrame
        :param ephoch_id: int
        
        :return: None
        """
        cachedDf = df.cache()
        fromAvroOptions = {"mode":"FAILFAST"}
        def getSchema(id):
            return str(self.__schema_registry_client.get_schema(id).schema_str)
        distinctValueSchemaIdDF = cachedDf.select(fn.col('valueSchemaId').cast('integer')).distinct()
        for valueRow in distinctValueSchemaIdDF.collect():
            currentValueSchemaId = self.spark.sparkContext.broadcast(valueRow.valueSchemaId)
            currentValueSchema = self.spark.sparkContext.broadcast(getSchema(currentValueSchemaId.value))
            filterValueDF = cachedDf.filter(fn.col('valueSchemaId') == currentValueSchemaId.value)
            micro_batch_id = str(uuid.uuid4())
            filterValueDF = filterValueDF.withColumn("batch_id", fn.lit(micro_batch_id))
            filterValueDF \
            .select('batch_id','topic', 'partition', 'offset', 'timestamp', 'timestampType', 'key', from_avro('fixedValue', currentValueSchema.value, fromAvroOptions).alias('parsedValue')) \
            .write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(self._deltaTablePath)
            
    def write_stream(self, queryname = "ParseStreamFromConfluent"):
        """
        Function used to write streamed and parsed data into delta lake.
        """
        self.streamingDf.writeStream \
            .option("checkpointLocation", self._checkpointsPath) \
            .foreachBatch(self.parse_stream_data) \
            .queryName(queryname) \
            .start()