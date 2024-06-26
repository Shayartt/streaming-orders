# First level imports:
from dataclasses import dataclass
from uuid import uuid4
import os

# Second level imports:

# Third party imports:
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

@dataclass
class KafkaProducer():
    topic: str
    schema: str # Path to avsc schema file
    serializer_function: callable
    debugg : bool = False
    
    def __post_init__(self):
        """
        Prepare Producer obj after creation
        """
        with open(self.schema) as f:
            schema_str = f.read()
            
        my_kafka_config = self.read_config(additional_details = True)
        schema_registry_conf = {
            "url" : my_kafka_config['schema_registry.url'],
            'basic.auth.user.info' : '{}:{}'.format(my_kafka_config['schema_registry.basic.auth.user.info'], my_kafka_config['schema_registry.basic.auth.pw'])
        }

        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        self.avro_serializer = AvroSerializer(schema_registry_client,
                                        schema_str,
                                        self.serializer_function)

        self.string_serializer = StringSerializer('utf_8')
        
        self.producer = Producer(self.read_config())

        print("Producing user records to topic {}. ^C to exit.".format(self.topic))
       
    def read_config(self, additional_details: bool = False):
        # reads the client configuration from client.properties
        # and returns it as a key-value map
        config = {}
        properties_path = "src/KafkaProducer/client.properties"
        if additional_details : 
            properties_path = "src/KafkaProducer/client_details.properties"
            
        with open(properties_path) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config
    
    def delivery_report(self, err, msg):
        """
        Reports the failure or success of a message delivery.

        Args:
            err (KafkaError): The error that occurred on None on success.

            msg (Message): The message that was produced or failed.

        Note:
            In the delivery report callback the Message.key() and Message.value()
            will be the binary format as encoded by any configured Serializers and
            not the same object that was passed to produce().
            If you wish to pass the original object(s) for key and value to delivery
            report callback we recommend a bound callback or lambda where you pass
            the objects along.
        """
        if err is not None:
            print("Delivery failed for record {}: {}".format(msg.key(), err))
            return
        print('record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))   
     
    def produce(self, list_of_messages: list):
        """
        Produce messages to Kafka topic
        """
        self.producer.poll(0.0)

        for my_message in list_of_messages:
            try:
                self.producer.produce(topic=self.topic,
                                key=self.string_serializer(str(uuid4())),
                                value=self.avro_serializer(my_message, SerializationContext(self.topic, MessageField.VALUE)),
                                on_delivery= self.delivery_report,
                                callback=self.delivery_report)
            except Exception as e :
                print("An error occurred: " + str(e))
                break
            
        self.producer.flush()