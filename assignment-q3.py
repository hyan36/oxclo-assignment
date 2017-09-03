from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.json('s3a://oxclo-assignment/dev200.json')

total = df.rdd.map(lambda row: 1).reduce(lambda a,b: a+b)

https_count = df.rdd.filter(lambda row: row.p443 is not None).map(lambda row: 1).reduce(lambda a,b: a+b)

http_count = df.rdd.filter(lambda row: row.p80 is not None).map(lambda row: 1).reduce(lambda a,b: a+b)

both = df.rdd.filter(lambda row: row.p80 is not None).filter(lambda row: row.p443 is not None).map(lambda row: 1).reduce(lambda a,b: a+b)

print float(https_count)/total
print float(http_count)/total
print float(both)/total

