import findspark
import val as val

findspark.init()

from pyspark import SparkContext, SparkConf

# val conf = new SparkConf()
#              .setMaster("local[2]")
#              .setAppName("CountingSheep")
# val sc = new SparkContext(conf)
val sc = new SparkContext(new SparkConf())