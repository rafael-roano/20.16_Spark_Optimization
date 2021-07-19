import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month

spark = SparkSession.builder.appName('Optimize I').getOrCreate()

answersDF = spark.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\MP\\Optimization\\data\\answers")
questionsDF = spark.read.parquet("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\MP\\Optimization\\data\\questions")

# answersDF.cache()
# spark.conf.set("spark.sql.adaptive.enabled","false")


answersDF.write.bucketBy(4, "question_id").mode("overwrite").saveAsTable("bucketed_answersDF")
bucketed_answers = spark.table("bucketed_answersDF")


# answers_month = answersDF.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))
answers_month = bucketed_answers.withColumn('month', month('creation_date')).groupBy('question_id', 'month').agg(count('*').alias('cnt'))

resultDF = questionsDF.join(answers_month, 'question_id').select('question_id', 'creation_date', 'title', 'month', 'cnt')

resultDF.orderBy('question_id', 'month').show()

# answersDF.unpersist()

