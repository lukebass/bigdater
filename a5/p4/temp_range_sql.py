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
    weather.createOrReplaceTempView("weather")

    tmin = spark.sql("SELECT * FROM weather WHERE qflag IS NULL AND observation == 'TMIN'")
    tmin.createOrReplaceTempView("tmin")
    tmax = spark.sql("SELECT * FROM weather WHERE qflag IS NULL AND observation == 'TMAX'")
    tmax.createOrReplaceTempView("tmax")

    range = spark.sql("SELECT tmin.station, tmin.date, ((tmax.value - tmin.value) / 10) AS range FROM tmin INNER JOIN tmax ON tmin.station = tmax.station AND tmin.date = tmax.date").cache()
    range.createOrReplaceTempView("range")

    maxRange = spark.sql("SELECT date, MAX(range) AS max FROM range GROUP BY date")
    maxRange.createOrReplaceTempView("maxRange")

    maxStation = spark.sql("SELECT range.date, range.station, range.range FROM range INNER JOIN maxRange ON range.date = maxRange.date AND range.range = maxRange.max ORDER BY range.date, range.station")
    maxStation.write.csv(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)