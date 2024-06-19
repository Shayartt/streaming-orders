import dataclasses


from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import window, col, when

@dataclasses.dataclass
class DataAnalyzer:
    spark: SparkSession
    
    def __post_init__(self):
        self.__MAX_ORDER_PER_HOUR: int = 6
        self._df = None
        self._input_folder: str = f"s3://labdataset/delta/orders_pipeline_2" # We will use the last pipeline, maybe make it dynamic later it's fien for now.
        self.analyzed = False
        
    def __str__(self):
        return f"Analyzer ready to analyze {self._df.count()} rows. \nData Loaded from : {self._input_folder}\n Status of analysis : {self.analyzed}"
    
    def set_input_folder(self, folder: str) -> None:
        """
        Set the input folder to load the data.
        """
        self._input_folder = folder
        
    def get_input_folder(self) -> str:
        """
        Get the input folder to load the data.
        """
        return self._input_folder
        
    def load_data(self):
        """
        Load the data from result folder.
        """
        self._df = self.spark.read.format("delta").load(self._input_folder)
        
    def analyze(self) -> None:
        """
        Applying some analysis on the data.
        
        1 - Fraud detection based on frequence of orders.
        2 - Customer segmentation based on the amount of orders.
        
        """
        # Start with fraud rules
        self.fraud_df = self.detect_fraud().toPandas()
        
        # if we have some frauds, we need to clean column windows and leave only start hour : 
        if self.fraud_df.shape[0] > 0:
            self.fraud_df["window"] = self.fraud_df["window"].apply(lambda x: x["start"])
            self.fraud_df["window"] = self.fraud_df["window"].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
            self.fraud_df["window_day"] = self.fraud_df["window"].apply(lambda x: x.split(" ")[0])
            self.fraud_df["window_hour"] = self.fraud_df["window"].apply(lambda x: x.split(" ")[1])
            # Group by window_day and window_hour and count the number of customers
            self.fraud_df = self.fraud_df.groupby(['window_day', 'window_hour']).size().reset_index(name='number_of_customers')
        
        # Then segment the customers: 
        self.segment_df = self.client_segmentation().toPandas()
        if self.segment_df.shape[0] > 0:
            self.segment_df = self.segment_df.groupby('segment')['count'].sum().reset_index()
        
        # Count per city and get top 10 cities with most orders.
        self.cities_per_orders = self.top_10_cities().toPandas()
        
        # Get TOP countries per total amount :
        self.countries_per_amount = self.top_countries().toPandas()

        # Total amount per payment method.
        self.amount_per_payment_method = self.total_amount_per_payment_method().toPandas()
        
        # Mark as analyzed.
        self.analyzed = True
        
    def detect_fraud(self, save_as_table: bool= True) -> SparkDataFrame: 
        """
        Group by customer_id in windows of 1 hour and count the number of orders, if higher than MAX_ORDER_PER_HOUR, it's fraud.
        """
        res = self._df.groupBy("customerId", window("orderDate", "1 hour")).count().filter(col("count") > self.__MAX_ORDER_PER_HOUR)
        
        if save_as_table: ## Overwrite because we load the whole data everytime. chekc raadme.md for more details/ recommendations.
            res.write.mode("overwrite").format("delta").saveAsTable("fraud_detection")
        
        return res
    
    def client_segmentation(self) -> SparkDataFrame:
        """
        Segment the customers based on the amount of orders.
        """
        return self._df.groupBy("customerId").count().withColumn("segment", 
                                                                  when(col("count") > 10, "VIP")
                                                                  .when(col("count") > 5, "Regular")
                                                                  .otherwise("Normal"))
        
    def top_10_cities(self):
        """
        Get the top 10 cities with most orders.
        """
        return self._df.groupBy("city").count().orderBy(col("count").desc()).limit(10)
    
    def top_countries(self):
        """
        Get the top countries with most total amount.
        """
        return self._df.groupBy("country").sum("totalAmountUSD").orderBy(col("sum(totalAmountUSD)").desc())
    
    def total_amount_per_payment_method(self):
        """
        Get the total amount per payment method.
        """
        return self._df.groupBy("paymentMethod").sum("totalAmountUSD").orderBy(col("sum(totalAmountUSD)").desc())