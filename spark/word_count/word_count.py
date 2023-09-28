spark-submit word_count.py 2>/dev/null

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
conf = SparkConf()
conf.setAppName('WordCount')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 9999)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)
wordCounts.pprint()
ssc.start() 
ssc.awaitTermination()
