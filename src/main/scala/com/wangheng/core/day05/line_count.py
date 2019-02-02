from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local","LineLength")
    lines = sc.textFile("/home/wangheng/Desktop/spark_test_data.txt", 1)
    pairs = lines.map(lambda x: (x, 1))
    linecounts = pairs.reduceByKey(lambda x, y: x+y)
    for x in linecounts.collect():
        print(x[0], " appear ", x[1], " times.")
        