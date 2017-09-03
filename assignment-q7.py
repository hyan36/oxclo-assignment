from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.json('s3a://oxclo-assignment/dev5000.json')
https = df.rdd.filter(lambda row: row.p443 is not None)
chiper_suites = https.filter(lambda row: row.p443.https.tls is not None and row.p443.https.tls.cipher_suite is not None).map(lambda row: (row.p443.https.tls.cipher_suite.name, 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], False)

for k,v in chiper_suites.collect()[:10]:
	print k,v
