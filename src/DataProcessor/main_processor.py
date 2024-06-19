from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from abc import ABC, abstractmethod
import dataclasses
from pyspark.sql.functions import col, lit, explode, broadcast, udf
from pyspark.sql.types import ArrayType, StringType

# Second level Import : 
from KafkaProducer.schema.example_customer import list_orders, supported_crypto
from utils import get_crypto_rates, get_add_details

#Third parties import :
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
    
    def __str__(self):
        return f"Pipeline {self.pipeline_id} ready to process {self._df.count()} rows. \nCall the .process() function to processed."
    
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
    
    @abstractmethod
    def mark_as_processed(self) :
        """
        Mark the data as processed.
        """
        pass
        # self._df = self._df.withColumn(f"PIPELINE_{self.pipeline_id}_processed", lit(True))
    
class Pipeline1(MainProcessor):
    def __init__(self, spark: SparkSession):
        super().__init__(1, spark)
    
    def __str__(self):
        return super().__str__()
        
    def process(self):
        # Get orders columns from the parsed value : 
        self._df = self._df.select(
            col('batch_id'),
            col('parsedValue.orderId').alias('orderId'),
            col('parsedValue.orderDate').alias('orderDate'),
            col('parsedValue.customerId').alias('customerId'),
            col('parsedValue.shippingAddress').alias('shippingAddress'),
            col('parsedValue.totalAmount').alias('totalAmount'),
            col('parsedValue.items').alias('items'),
            col('parsedValue.paymentMethod').alias('paymentMethod'),
            col('parsedValue.paymentStatus').alias('paymentStatus'),
            col('parsedValue.orderStatus').alias('orderStatus')
        )
        # Here the items columns is a list of dict, we need to explode it :
        self._df = self._df.withColumn("item", explode(col("items")))
        
        # Instead of leaving it as a dict, we'll extract the productId and quantity from it :
        self._df = self._df.withColumn("productId", col("item.productId"))
        self._df = self._df.withColumn("quantity", col("item.quantity"))
        
        # We'll drop the item column now :
        self._df = self._df.drop("items", "item")
        
        # Load our product's details from our dataset.
        self.get_products_details()
                      
        # IDEAS : You have payment method that can be lot of options you need to convert everything to USD
        self.convert_amount()
        
        self.mark_as_processed()
        
    def load_data(self):
        super().load_data()
    
    def clean_processed(self, batch_id: str) -> bool:
        return super().clean_processed(batch_id)
    
    def write_data(self):
        super().write_data()
    
    def get_products_details(self):
        """
        Used to load the products details from our dataset and merge it to main dataframe.
        """
        # We'll need to gather more information about the item from our dataset, we'll load it then join it :
        df_products = self.load_products()  

        # Joining dataframes : 
        self._df = self._df.join(broadcast(df_products), self._df.productId == df_products.item_id, "left")  
    
    def convert_amount(self) : 
        """
        Based on the payment method if it's crypto we'll convert it into usd, (we'll considering it's USD if it's not anyways)
        """
        df_rates = self.load_crypto_rates()
        
        # Joining dataframes :
        self._df = self._df.join(broadcast(df_rates), self._df.paymentMethod == df_rates.crypto_currency, "left")
        
        # For null values we'll consider it's USD
        self._df = self._df.fillna(1, subset=["usd_rate"])
        
        # We'll convert the total amount to USD
        self._df = self._df.withColumn("totalAmountUSD", col("totalAmount") * col("usd_rate"))
     
    def load_crypto_rates(self) -> SparkDataFrame:
        """
        Function used to load the exchange rates (amount) of supported crypto currencies to USD.
        """  
        res = {}
        for crypto_currency in supported_crypto : 
            my_rate = get_crypto_rates((crypto_currency).lower())
            res[crypto_currency] = float(my_rate)

            
        # Transform the dictionary into a spark dataframe : 
        columns = ["crypto_currency", "usd_rate"]
        return self.spark.createDataFrame(list(res.items()), columns)
        
    def load_products(self) -> SparkDataFrame: 
        """
        This function will load the products dataset in our example it's a simple python dict but in real life it will be a dataset that needs to be loaded from a datalake or datawarehouse.
        """
        # Transform the dictionary into a list of tuples, here I could've used the name instead of id, just wanted to experience more with the data.
        product_data = [( "P" + str(k.zfill(3)), v["name"], v["unit_price"]) for k, v in list_orders.items()]

        # Define the schema
        columns = ["item_id", "item_name", "item_unit_price"]

        return self.spark.createDataFrame(product_data, columns)
                
    def mark_as_processed(self) :
        super().mark_as_processed()
        
    
    
class Pipeline2(MainProcessor):
    def __init__(self, spark: SparkSession):
        super().__init__(2, spark)
        
    def __str__(self):
        return super().__str__()
    
    def process(self):
        # We'll get the shippiment details (like city, postal code and country) from the shipping address.
        self.get_shipping_details()
        
        self.mark_as_processed()
     
    def get_shipping_details(self): 
        """
        Function will use the geopy to get information regarding the shipping address.
        """
        
        # We'll use the function get_add_details to get the city, postal code and country from the shipping address.
        # Initialize the UDF function :
        get_add_details_udf = udf(get_add_details, ArrayType(StringType()))
        
        # To optimize the execution time we'll get the distinct address as a seperate dataframe and broadcast it.
        df_address = self._df.select("shippingAddress").distinct()
        
        # Initialize the columns with Unknown values :
        df_address = df_address.withColumn("city", lit("Unknown"))
        df_address = df_address.withColumn("postal_code", lit("Unknown"))
        df_address = df_address.withColumn("country", lit("Unknown"))
        
        # Apply the UDF function to get the informations
        df_address = df_address.withColumn("shipping_details", get_add_details_udf(col("shippingAddress")))
        
        # Split the information into differents columns :
        df_address = df_address.withColumn("city", col("shipping_details")[0])
        df_address = df_address.withColumn("postal_code", col("shipping_details")[1])
        df_address = df_address.withColumn("country", col("shipping_details")[2])
        
        # Drop the column shipping_details :
        df_address = df_address.drop("shipping_details")
        
        # Join the dataframes, by doing this instead of calling the function for each row we'll call it only once for each distinct address.
        self._df = self._df.join(broadcast(df_address), "shippingAddress", "left")
        
    def load_data(self):
        super().load_data()
    
    def clean_processed(self, batch_id: str) -> bool:
        return super().clean_processed(batch_id)
    
    def write_data(self):
        super().write_data()
        
    def mark_as_processed(self) :
        super().mark_as_processed()
