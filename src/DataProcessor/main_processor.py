from pyspark.sql import SparkSession
from abc import ABC, abstractmethod
import dataclasses
from pyspark.sql.functions import col, lit

from delta.tables import DeltaTable

@dataclasses.dataclass
class MainProcessor(ABC):
    """
    Mother class for our processors pipelines, it's an abstract class
    """
    pipeline_id : int 
    spark: SparkSession
    
    def __post_init__(self):
        if self.pipeline_id == 1 : 
            self.input_folder = "s3://labdataset/delta/orders"
        else : 
            self.input_folder = f"s3://labdataset/delta/orders_pipeline_{self.pipeline_id-1}"
            
        self._df = None
    
    @abstractmethod
    def load_data(self):
        """
        Load the data from previous folder.
        """
        self._df = self.spark.read.format("delta").load(self.input_folder)
    
    @abstractmethod
    def clean_processed(self, batch_id: str) -> bool:
        """
        Clean the processed data from the input folder using the batch_id and delta tables.
        """
        # Remove processed data from the original table
        delta_table = DeltaTable.forPath(self.spark, self.input_folder)
        delta_table.delete(condition=col("batch_id") == batch_id)
        
        return True
    
    @abstractmethod
    def write_data(self):
        """
        Write the data into next folder.
        """
        self._df.write \
        .format("delta") \
        .mode("append") \
        .save(f"s3://labdataset/delta/orders_pipeline_{self.pipeline_id}")
 
        # Clean the processed data using distinct batch_id : 
        my_unique_ids = self._df.select("batch_id").distinct()
        
        # For each batch_id we'll call self.clean_processed:
        for row in my_unique_ids.collect():
            self.clean_processed(row.batch_id)
            
        print("Data cleaned and written.")
    
    @abstractmethod
    def process(self):
        """
        Process the data, here all the logic in our pipeline.
        """
        pass
    
    
class Pipeline1(MainProcessor):
    def __init__(self, spark: SparkSession):
        super().__init__(1, spark)
        
    def process(self):
        # IDEAS : You have payment method that can be lot of options you need to convert everything to USD
        self._df = self._df.withColumn("processed", lit(True))
        
    def load_data(self):
        super().load_data()
    
    def clean_processed(self, batch_id: str) -> bool:
        return super().clean_processed(batch_id)
    
    def write_data(self):
        super().write_data()
        
    
class Pipeline2(MainProcessor):
    def __init__(self, spark: SparkSession):
        super().__init__(2, spark)
        
    def process(self):
        # IDEAS : Get city and postal code from shipping address
        self._df = self._df.withColumn("processed2", lit(True))
        
    def load_data(self):
        super().load_data()
    
    def clean_processed(self, batch_id: str) -> bool:
        return super().clean_processed(batch_id)
    
    def write_data(self):
        super().write_data()
        
