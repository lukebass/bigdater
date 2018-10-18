from pyspark import SparkConf, SparkContext
import sys, os
assert sys.version_info >= (3, 5)
sc = SparkContext(conf=SparkConf().setAppName('shortest path'))
assert sc.version >= '2.3'

def main(inputs, output, start, end):
    path = os.path.join(inputs, 'links-simple-sorted.txt')
    fields = sc.textFile(path).map(getEdges).cache()
    
    paths = sc.parallelize([(start, (None, 0))])
    for i in range(2):
        current = paths.filter(lambda x: x[1][1] == i)
        neighbours = fields.join(current).map(lambda x: (x[0], x[1][0]))
        paths = neighbours.flatMap(lambda x: getPaths(x, i + 1)).union(paths)
        paths = paths.reduceByKey(min)
       
    print(paths.collect())
    #paths.saveAsTextFile(output + '/iter-' + str(i))

def getPaths(node, distance):
    for path in node[1]:
        yield (path, (node[0], distance))

def getEdges(node):
    fields = node.split()
    key = fields[0][:-1]

    nodes = []
    for i in range(1, len(fields)):
        nodes.append(fields[i])

    return (key, nodes)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    start = sys.argv[3]
    end = sys.argv[4]
    main(inputs, output, start, end)