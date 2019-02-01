from pyspark import SparkContext

def reduce():
    sc = SparkContext('local', 'reduce action')
    rdd = sc.parallelize(range(0, 10))
    result = rdd.reduce(lambda x, y: x+y)
    print(result)

def collect():
    sc = SparkContext('local', 'reduce action')
    rdd = sc.parallelize(range(0, 10))
    resultRDD = rdd.map(lambda x: x*2)
    print(resultRDD.collect())

def count():
    sc = SparkContext('local', 'reduce action')
    rdd = sc.parallelize(range(0, 10))
    print(rdd.count())

def take():
    sc = SparkContext('local', 'reduce action')
    rdd = sc.parallelize(range(0, 10))
    result_collect = rdd.take(5)
    for i in result_collect:
        print(i, end=" ")

def countByKey():
    sc = SparkContext('local', 'countByKey')
    rdd = sc.parallelize([('class1', 'wangheng'), ('class1', 'huanhuan'), ('class3', 'kdlsdlds')], 1)
    result = rdd.countByKey()
    print(result)

if __name__ == '__main__':
    # reduce()
    # collect()
    # count()
    # take()
    countByKey()