from pyspark import SparkConf, SparkContext
import sys, operator, random
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(samples, numSlices):
    totalIterations = sc.parallelize([samples // numSlices] * numSlices, numSlices).map(getIterations).sum()
    print(totalIterations/samples)

def getIterations(slice):
    random.seed()
    itr = 0

    for i in range(0, slice):
        sum = 0.0
        while sum < 1:
            sum += random.random()
            itr += 1

    return itr

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+

    numSlices = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    main(int(sys.argv[1]), numSlices)