from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

df = sqlContext.read.json('s3a://oxclo-assignment/dev5000.json')
ssh = df.rdd.filter(lambda row: row.p22 is not None and row.p22.ssh is not None and row.p22.ssh is not None and row.p22.ssh.banner is not None)
software = ssh.filter(lambda row: row.p22.ssh.banner.server_id is not None and row.p22.ssh.banner.server_id.software is not None and row.p22.ssh.banner.server_id.version is not None)

software_version = software.map(lambda row: (row.p22.ssh.banner.server_id.software + "-" +row.p22.ssh.banner.server_id.version, 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], False)
result = software_version.collect()[:1]
for k,v in result:
	print k,v


