from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext('local', 'wordcount')
    lines = sc.textFile('/home/wangheng/Desktop/spark_test_data.txt', 1)
    words = lines.flatMap(lambda line: line.split(" "))
    paris = words.map(lambda x: (x, 1))
    count = paris.reduceByKey(lambda x, y: x+y)
    for x in count.collect():
        print(x[0], " appear ", x[1], " times!")