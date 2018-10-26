from pyspark.sql import SparkSession, functions, types, Row
import sys, re, datetime, uuid
assert sys.version_info >= (3, 5)
spark = SparkSession.builder.appName('nasa logs').config('spark.cassandra.connection.host', '199.60.17.188, 199.60.17.216').getOrCreate()
assert spark.version >= '2.3'

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(inputs, keyspace, table):
    schema = types.StructType([
        types.StructField('id', types.StringType(), True),
        types.StructField('host', types.StringType(), True),
        types.StructField('datetime', types.DateType(), True),
        types.StructField('path', types.StringType(), True),
        types.StructField('bytes', types.IntegerType(), True)
    ])

    fields = spark.sparkContext.textFile(inputs).flatMap(getFields)
    data = spark.createDataFrame(fields, schema)
    data.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).save()

def getFields(line):
    fields = wordsep.split(line)
    if (len(fields) > 3):
        yield (str(uuid.uuid4()), fields[1], datetime.datetime.strptime(fields[2], '%d/%b/%Y:%H:%M:%S'), fields[3], int(fields[4]))

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    main(inputs, keyspace, table)