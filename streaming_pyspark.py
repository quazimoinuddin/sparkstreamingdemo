from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":

	sparkSession = SparkSession.builder.appName("Spark Streaming") \
		                               .getOrCreate()

	sparkSession.sparkContext.setLogLevel("ERROR")

	schema = StructType([StructField("age", IntegerType(), False), \
    					 StructField("sex", StringType(), False), \
    					 StructField("bmi", DoubleType(), False), \
    					 StructField("children", IntegerType(), False), \
    					 StructField("smoker", StringType(), False), \
    					 StructField("region", StringType(), False), \
    					 StructField("charges", DoubleType(), False)])


	fileStreamDf = sparkSession.readStream\
					 		   .option("header", "true")\
					 		   .schema(schema)\
					 		   .csv("datasets/")

	print("*************")
	print("Is the stream ready? ", fileStreamDf.isStreaming)

	print("*************")
	print("Stream schema ", fileStreamDf.printSchema())

	selectedDf = fileStreamDf.groupBy("sex", "smoker")\
							 .agg({"charges": "avg"})\
							 .withColumnRenamed("avg(charges)", "Average Charges")

	query = selectedDf.writeStream\
					  .outputMode("complete")\
					  .format("console")\
					  .option("numRows", 30)\
					  .start()

	query.awaitTermination()
