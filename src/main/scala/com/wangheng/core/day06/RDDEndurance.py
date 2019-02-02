from pyspark import SparkContext

def persist():
    sc = SparkContext('local', 'rdd endurance')
    rdd = sc.textFile("/home/wangheng/Desktop/test_data/test_data2.txt", 1).cache()
    print(rdd.count())

def broadcast():
    sc = SparkContext('local', 'broadcast')
    rdd = sc.textFile('/home/wangheng/Desktop/test_data/test_data3.txt', 1)
    numbersRDD = rdd.flatMap(lambda x: x.split(' '))
    broadcast_value = sc.broadcast(5)
    result_num = numbersRDD.map(lambda x: int(x) * broadcast_value.value)
    print(result_num.collect())

if __name__ == '__main__':
    # persist()
    broadcast()