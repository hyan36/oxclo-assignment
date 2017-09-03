from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.json('s3a://oxclo-assignment/dev5000.json')
total = df.rdd.map(lambda row: 1).reduce(lambda a,b: a+b)

has_meta = df.rdd.filter(lambda row: row.metadata is not None and row.metadata.os is not None)
windows = has_meta.filter(lambda row: row.metadata.os is not None and row.metadata.os.lower() == "windows").map(lambda row: 1).reduce(lambda a,b: a+b)
ubuntu = has_meta.filter(lambda row: row.metadata.os is not None and row.metadata.os.lower() == "ubuntu").map(lambda row: 1).reduce(lambda a,b: a+b)

print windows
print ubuntu

