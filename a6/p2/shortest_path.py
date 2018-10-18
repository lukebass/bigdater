from pyspark import SparkConf, SparkContext
import sys, os
assert sys.version_info >= (3, 5)
sc = SparkContext(conf=SparkConf().setAppName('shortest path'))
assert sc.version >= '2.3'

def main(inputs, output, start, end):
    path = os.path.join(inputs, 'links-simple-sorted.txt')
    fields = sc.textFile(path).map(getEdges).cache()
    
    paths = sc.parallelize([(start, (None, 0))])
    for i in range(6):
        current = paths.filter(lambda x: x[1][1] == i)
        newPaths = fields.join(current).flatMap(getPaths)
        paths = paths.union(newPaths).reduceByKey(min)

        if paths.lookup(end):
            break
    
    finalPath = [end]
    source = paths.lookup(end)[0][0]
    while source is not None:
        finalPath.insert(0, source)
        source = paths.lookup(source)[0][0]

    finalPath = sc.parallelize(finalPath, 1)
    finalPath.saveAsTextFile(output + '/path')

def getPaths(node):
    neighbours = node[1][0]
    distance = node[1][1][1]

    for neighbour in neighbours:
        yield (neighbour, (node[0], distance + 1))

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