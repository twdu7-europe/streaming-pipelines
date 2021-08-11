import pyspark
sc = pyspark.SparkContext('local[*]')

sc.range(1, 1000).count()