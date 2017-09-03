from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json('s3a://oxclo-assignment/dev5000.json')
total = df.rdd.map(lambda row: 1).reduce(lambda a,b: a+b)
https = df.rdd.filter(lambda row: row.p443 is not None)
heartbleeds = https.filter(lambda row: row.p443.https.heartbleed is not None and row.p443.https.heartbleed.heartbleed_vulnerable )
count = heartbleeds.map(lambda row: 1).reduce(lambda a,b: a+b)
print float(count)/total

