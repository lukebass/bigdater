from pyspark import SparkConf, SparkContext
import sys, operator, random
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def main(samples):
    totalIterations = sc.range(0, samples).map(generateE).reduce(operator.add)
    print(totalIterations/samples)

def generateE(sample):
    random.seed()
    sum = 0.0
    itr = 0
    while sum < 1:
        sum += random.random()
        itr += 1
    
    return itr

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    samples = int(sys.argv[1])
    main(samples)