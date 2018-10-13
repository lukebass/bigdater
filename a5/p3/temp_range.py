from pyspark.sql import SparkSession, functions, types
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
spark = SparkSession.builder.appName('weather').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+

def main(inputs, output):
    observation_schema = types.StructType([
        types.StructField('station', types.StringType(), False),
        types.StructField('date', types.StringType(), False),
        types.StructField('observation', types.StringType(), False),
        types.StructField('value', types.IntegerType(), False),
        types.StructField('mflag', types.StringType(), False),
        types.StructField('qflag', types.StringType(), False),
        types.StructField('sflag', types.StringType(), False),
        types.StructField('obstime', types.StringType(), False)
    ])

    weather = spark.read.csv(inputs, schema=observation_schema).cache()
    tmin = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMIN').withColumnRenamed('value', 'min')
    tmax = weather.filter(weather.qflag.isNull()).filter(weather.observation == 'TMAX').withColumnRenamed('value', 'max')
    range = tmin.join(tmax, ['station', 'date']).withColumn('range', (tmax.max - tmin.min) / 10).cache()

    maxRange = range.groupBy('date').max('range').withColumnRenamed('max(range)', 'range')
    maxStation = range.join(maxRange, ['date', 'range']).orderBy('date', 'station')
    maxStation[['date', 'station', 'range']].write.csv(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)