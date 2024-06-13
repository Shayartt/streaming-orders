from pyspark.sql import SparkSession
from abc import ABC, abstractmethod

class MainStreamLoader(ABC) :
    """
    This class represent the main stream loader, it's an abstract class
    """
    @abstractmethod
    def read_stream(self):
        """
        Start reading the stream
        """
        pass
    
    @abstractmethod
    def write_stream(self):
        """
        Start writing the stream
        """
        pass
    
    @abstractmethod
    def parse_stream_data(self):
        """
        Parse the stream data
        """
        pass