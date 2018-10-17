from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
sc = SparkContext(conf=SparkConf().setAppName('shortest path'))
assert sc.version >= '2.3'


def main(inputs, output, start, end):
    # main logic starts here

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    main(inputs, output)