from __future__ import print_function
import findspark


findspark.init()

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('MyFirstStandaloneApp').setMaster("local[4]").setAll(
[('spark.executor.memory', '6g'),
 ('spark.driver.memory', '4g'),
 ('spark.executor.heartbeatInterval', '3s'),
 ('spark.driver.extraJavaOptions', '-XX:+UseG1GC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps'),
 ('spark.executor.extraJavaOptions', '-XX:+UseG1GC -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps')])

sc = SparkContext(conf=conf)


text_file = sc.textFile("../shakespear.txt")

countsBigWords = text_file.flatMap(lambda line: line.split(" ")) \
                  .filter(lambda word: len(word) > 10) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)

print ("Number of words with more than 10 letters: " + str(countsBigWords.count()))

countsLetter = text_file.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, len(word))) \
                  .reduceByKey(lambda a, b: a + b)

print ("Number of letters: " + str(countsLetter.count()))
# counts.saveAsTextFile("./shakespeareWordCount")

top5wordsList = text_file.flatMap(lambda line: line.split("\\s")) \
                  .map(lambda word: (len(word), word)) \
                  .sortByKey(ascending= False) \
                  .take(5) \
                  .collect()
                   # .foreach(print)
print(top5wordsList[:])
