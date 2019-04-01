from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext("local[*]", "Text Length")
    lines = sc.textFile("hdfs://localhost:9000/spark_test_data.txt", 1)
    line_count = lines.map(lambda x:len(x))
    count = line_count.reduce(lambda x, y: x+y)
    print("file's length is : ", count)
