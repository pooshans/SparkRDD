### Introduction:
This is basically implementing the exposed API's which Spark RDD extends. Spark provides plug-in facility of external custom datasource model into Spark Resilient Distributed Datasets (RDD) ecosystem which we basically have incorporated through this implementation. The example implemented is for better understanding which will enable the developer to implement his/her usecase according to their own requirement. Currently, we have provided support of text file while for other data type, we must have some own reader to read block or records.

This implementation is inspired by [Spark Research Paper](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

### Underlaying conceptual design:
You can find my analysis of the above spark research paper [here](https://github.com/pooshans/SparkRDD/blob/master/Spark2.0.2%20_%20Extending%20Rich%20API.docx)
 
### Installation guidelines.
1. Clone the project.
2. You should have java 8 version or higher.
3. mvn clean install
4. Run the *SparkExtendExample* main java class.

##### limitations:
1. Currently I have provided implementation for java but it needs to be extended for pyspark.
2. Currently, programe perform for simple text file or binary file as we have BufferedReader. We can do so for other's datasource as well if we have such supported reader.
