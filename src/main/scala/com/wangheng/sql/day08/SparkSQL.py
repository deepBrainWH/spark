from pyspark.sql import SparkSession

if __name__ == '__main__':
    url = "jdbc:mysql://localhost:3306/program1?user=root;password=mysql673"

    spark = SparkSession.builder.appName("jdbc data sources").master('local').getOrCreate()
    jdbcDF = spark.read.format('jdbc').option('url', url)\
    .option('dbtable', 'Course')\
    .option('user', 'root')\
    .option('password', 'mysql673')\
    .option('dirver', 'com.mysql.cj.jdbc.Driver')\
    .load()

    jdbcDF.printSchema()
    jdbcDF.show()