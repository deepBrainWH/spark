from pyspark import SparkContext
if __name__ == '__main__':
    sc = SparkContext('local[*]', 'parallelize_collection')
    sc.setLogLevel("ERROR")
    a = [1,2,3,4,5,6,7,8,9,10]
    rdd = sc.parallelize(a)
    sum = rdd.reduce(lambda x, y:(x+y))
    print(sum)