# streaming-orders
Project to stream real-time orders and apply some ETL pipelines &amp; analytics using DataBricks, AWS

### Documentation Diagram : 

#### High Level Diagram Tec:
![High-Level Diagram](docs/diagrams/streaming-orders-highlevel.drawio.png?raw=true "High-Level")

#### Data Flow : 
![Data Flow Diagram](docs/diagrams/Low-level-data-flow.drawio.png?raw=true "Data Flow")


More details to be included later...

### Milestones : 

+ Stream orders from Kafka: Simulate a real-time stream of orders from an e-commerce platform. (Kafka Confluent) ✅
    - Kafka Setup in confluent.
         ![KafkaTopic](docs/screens/kafka_topic.png?raw=true "KafkaTopic")
    - Producer simulation in src/simulator.py
+ Streaming ingesting & processing : Create spark jobs to read from our kafka stream, apply etl pipelines and write our data into a delta lake (Databricks workflows + AWS S3) ✅
    - Streaming :  🚀
        ![Streamingflow](docs/screens/streaming_workflow.PNG?raw=true "Streamingflow")
    - Processing : 🛠️
        ![Processingflow](docs/screens/processing_workflow.png?raw=true "Processingflow")
+ Analytics Pipelines : the final step of any workflow triggered on new orders received need to apply some analysis tasks as mentionned bellow :✅
    - Develop a fraud detection rules that flags suspicious orders based on patterns such as high-value orders, frequent orders from the same customer, etc. 💡
    - Alerting: Set up alerts for flagged suspected clients using Databricks alerting systems (email only). 🚨
        + <span style="color:blue"> Done using Databricks SQL Alerts, the analyzer code will store results into a delta table, and via databricks platform we setup trigger based on that table.</span>
            ![Fraud Rule](docs/screens/Fraud_alerts.png?raw=true "Fraud Rule")

+ Vizualization : It's not the main objective of this project, but we'll create a quick dashboard included some analytical statistics using databricks dashboarding. 👀

 
+ Additional Challenges : as this is a learning project, I decided to add some challenging use-cases to simulate real-life problems as mentionned bellow : 🤓
    -  Data Schema Evolution: Handle changes in the data schema over time, such as new fields being added to the order data, and changing types. 🔄
    -  Data Schema Evolution: Ensure backward and forward compatibility in our streaming pipeline. 🔄
        + <span style="color:blue"> I've added a new field called "message" in my topic and changed the producer code to include it from one side, from the other side I had to enable the merge schema to allow schema evolution in my delta table, to allow backward compatibility I provided a default value to my new field. </span>

    -  Exactly-Once Processing: Implement exactly-once processing semantics to ensure that each order is processed exactly once, even in the case of failures. (Avoid dupplication)
    -  Stateful Stream Processing: Use stateful transformations to keep track of state across events. For example, maintain a running total of orders per customer or product category.
        + Implemented two live counters in statefull_streaming.ipynb.
    -  Scalability and Performance Optimization: Optimize your Spark streaming jobs for performance and scalability. This includes tuning Spark configurations, partitioning data effectively, and minimizing data shuffles.
    -  Data Quality Monitoring: Implement real-time data quality checks and monitoring. Alert if data quality issues such as missing values, duplicates, or outliers are detected.
    -  Data Enrichment: Enrich your streaming data with additional context from external data sources. For example, enrich order data with customer demographics or product details from a database.
    -  Real-Time Analytics and Reporting: Build real-time analytics and reporting dashboards using tools like Databricks SQL, Power BI, or Tableau. Visualize key metrics such as order volume, sales trends, and customer behavior.
 

