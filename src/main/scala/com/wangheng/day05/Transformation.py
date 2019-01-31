from pyspark import SparkContext

def map():
    sc = SparkContext("local", "Transformation")
    numbersRDD = sc.parallelize([1,2,3,4,5,6,7])
    multipleNumberRDD = numbersRDD.map(lambda x: x*2)
    foreachRDD(multipleNumberRDD)

def filter():
    sc = SparkContext("local", "Transformation")
    numbersRDD = sc.parallelize([1,2,3,4,5,6,7])
    filterrdd = numbersRDD.filter(lambda x: x % 2 == 0)
    foreachRDD(filterrdd)

def flatMap():
    sc = SparkContext("local", "Transformation")
    rdd = sc.textFile("/home/wangheng/Desktop/test_data/spark_test_data.txt", 1)
    words = rdd.flatMap(lambda line: line.split(" "))
    foreachRDD(words)

def groupByKey():
    sc = SparkContext("local", "Transformation")
    rdd = sc.parallelize([('class1', 89), ('class2', 88), ('class1', 78), ('class2', 99)])
    scores = rdd.groupByKey()
    print(scores.mapValues(list).collect())

def foreachRDD(rdd):
    for x in rdd.collect():
        print(x)

if __name__ == '__main__':
    # map()
    # filter()
    # flatMap()
    groupByKey()

