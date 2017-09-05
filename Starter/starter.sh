export AWS_ACCESS_KEY_ID=<Key>
export AWS_SECRET_ACCESS_KEY=<Secret>

pyspark --master spark://0.0.0.0:7077 --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2
  q1.py
  
