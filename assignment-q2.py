from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json('s3a://oxclo-assignment/dev.json')

countries = df.rdd.filter(lambda row: row.location is not None)\
.map(lambda row: (row.location.country_code, 1))\
.reduceByKey(lambda a,b: a+b)\
.sortBy(lambda x: x[1], False)

for k,v in countries.collect()[:10]:
	print k,v


