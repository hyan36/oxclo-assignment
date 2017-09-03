from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json('s3a://oxclo-assignment/dev.json')
count = df.rdd.map(lambda row: 1).reduce(lambda a,b: a+b)
print count



