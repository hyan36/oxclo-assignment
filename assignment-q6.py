from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.json('s3a://oxclo-assignment/dev5000.json')

has_meta = df.rdd.filter(lambda row: row.metadata is not None and row.metadata.os is not None)
total_os = has_meta.map(lambda row: 1).reduce(lambda a,b: a+b)
windows = has_meta.filter(lambda row: row.metadata.os is not None and row.metadata.os.lower() == "windows").map(lambda row: 1).reduce(lambda a,b: a+b)
ubuntu = has_meta.filter(lambda row: row.metadata.os is not None and row.metadata.os.lower() == "ubuntu").map(lambda row: 1).reduce(lambda a,b: a+b)

print float (windows)/total_os
print float (ubuntu)/total_os

