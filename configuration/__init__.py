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

counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)

print ("Number of elements: " + str(counts.count()))
# counts.saveAsTextFile("./shakespeareWordCount.txt")