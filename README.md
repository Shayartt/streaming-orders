# streaming-orders
Project to stream real-time orders and apply some ETL pipelines &amp; analytics using DataBricks, AWS

### Documentation Diagram : 

#### High Level Diagram Tec:
![High-Level Diagram](docs/diagrams/streaming-orders-highlevel.drawio.png?raw=true "High-Level")

More details to be included later...

### Milestones : 

+ Stream orders from Kafka: Simulate a real-time stream of orders from an e-commerce platform. (Kafka Confluent)
+ Streaming ingesting & processing : Create spark jobs to read from our kafka stream, apply etl pipelines and write our data into a delta lake (Databricks workflows + AWS S3)
+ Analytics Pipelines : the final step of any workflow triggered on new orders received need to apply some analysis tasks as mentionned bellow :
    - Develop a fraud detection algorithm that flags suspicious orders based on patterns such as high-value orders, frequent orders from the same IP, etc.
    - Alerting: Set up real-time alerts for flagged orders using Databricks and integration with alerting systems (email only).
 
+ Additional Challenges : as this is a learning project, I decided to add some challenging use-cases to simulate real-life problems as mentionned bellow :
    -  Data Schema Evolution: Handle changes in the data schema over time, such as new fields being added to the order data, and changing types.
    -  Data Schema Evolution: Ensure backward and forward compatibility in our streaming pipeline.
    -  Exactly-Once Processing: Implement exactly-once processing semantics to ensure that each order is processed exactly once, even in the case of failures. (Avoid dupplication)
    -  Stateful Stream Processing: Use stateful transformations to keep track of state across events. For example, maintain a running total of orders per customer or product category.
    -  Scalability and Performance Optimization: Optimize your Spark streaming jobs for performance and scalability. This includes tuning Spark configurations, partitioning data effectively, and minimizing data shuffles.
    -  Data Quality Monitoring: Implement real-time data quality checks and monitoring. Alert if data quality issues such as missing values, duplicates, or outliers are detected.
    -  Data Enrichment: Enrich your streaming data with additional context from external data sources. For example, enrich order data with customer demographics or product details from a database.
    -  Real-Time Analytics and Reporting: Build real-time analytics and reporting dashboards using tools like Databricks SQL, Power BI, or Tableau. Visualize key metrics such as order volume, sales trends, and customer behavior.
 

