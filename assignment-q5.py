from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json('s3a://oxclo-assignment/dev5000.json')

https = df.rdd.filter(lambda row: row.p443 is not None and row.location is not None)
heartbleeds = https.filter(lambda row: row.p443.https.heartbleed is not None and row.p443.https.heartbleed.heartbleed_vulnerable )
countries = heartbleeds.map(lambda row: (row.location.country_code, 1)).reduceByKey(lambda a,b: a+b)
total_https = https.map(lambda row: (row.location.country_code, 1)).reduceByKey(lambda a,b: a+b)

percent = countries.join(total_https).map(lambda row: (row[0], float(row[1][0])/row[1][1])).sortBy(lambda x: x[1], False)
for k,v in percent.collect()[:10]:
	print k,v


