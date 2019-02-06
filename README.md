###### Introduction:
This is basically implementing the exposed API's which Spark RDD extends. Spark provides plug-in facility of external custom datasource model into Spark Resilient Distributed Datasets (RDD) ecosystem which we basically have incorporated through this implementation. The example implemented is for better understanding which will enable the developer to implement his/her usecase according to their own requirement. Currently, we have provided support of text file while for other data type, we must have some own reader to read block or records.
 
###### Installation guidelines.
1. Clone the project.
2. You should have java 8 version or higher.
3. mvn clean install
4. Run the *SparkExtendExample* main java class.
