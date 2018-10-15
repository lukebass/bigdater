from pyspark.sql import SparkSession, functions, types, Row
import sys, re
assert sys.version_info >= (3, 5)
spark = SparkSession.builder.appName('nasa logs').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(inputs):
    schema = types.StructType([
        types.StructField('host', types.StringType(), True),
        types.StructField('bytes', types.StringType(), True)
    ])

    fields = spark.sparkContext.textFile(inputs).flatMap(getFields)
    data = spark.createDataFrame(fields, schema)
    totals = data.groupBy('host').sum('bytes')
    #print(data.head(5))

def getFields(line):
    fields = wordsep.split(line)
    if (len(fields) > 3):
        yield Row(host=fields[1], bytes=int(fields[4]))

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)