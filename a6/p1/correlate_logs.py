from pyspark.sql import SparkSession, functions, types, Row
import sys, re, math
assert sys.version_info >= (3, 5)
spark = SparkSession.builder.appName('nasa logs').getOrCreate()
assert spark.version >= '2.3'

wordsep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def main(inputs):
    schema = types.StructType([
        types.StructField('host', types.StringType(), True),
        types.StructField('bytes', types.IntegerType(), True)
    ])

    fields = spark.sparkContext.textFile(inputs).flatMap(getFields)
    data = spark.createDataFrame(fields, schema)

    totals = data.groupBy('host').agg(functions.count('*').alias('x'), functions.sum('bytes').alias('y')).drop('host')
    totals = totals.withColumn('n', functions.lit(1)).withColumn('x^2', totals.x**2).withColumn('y^2', totals.y**2).withColumn('xy', totals.x * totals.y)
    sums = totals.groupBy().sum().head()
    
    r = (sums['sum(n)'] * sums['sum(xy)'] - sums['sum(x)'] * sums['sum(y)']) / (math.sqrt(sums['sum(n)'] * sums['sum(x^2)'] - sums['sum(x)']**2) * math.sqrt(sums['sum(n)'] * sums['sum(y^2)'] - sums['sum(y)']**2))
    print('r = ' + str(r))
    print('r^2 = ' + str(r**2))

def getFields(line):
    fields = wordsep.split(line)
    if (len(fields) > 3):
        yield Row(host=fields[1], bytes=int(fields[4]))

if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)