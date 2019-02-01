from pyspark import SparkContext

if __name__ == '__main__':
    sc = SparkContext('local', 'sortwordcount')
    lines = sc.textFile("/home/wangheng/Desktop/test_data/test_data4.txt", 1)
    words = lines.flatMap(lambda line: line.split(" "))
    wordmap = words.map(lambda word: (word, 1))
    wordcount = wordmap.reduceByKey(lambda x, y:x+y)
    countword = wordcount.map(lambda word:(word[1], word[0]))
    result = countword.sortByKey(False)
    result = result.map(lambda wordmap: (wordmap[1], wordmap[0]))
    print(result.collect())