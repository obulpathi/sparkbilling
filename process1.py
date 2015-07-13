from pyspark import SparkContext

def loadLogs(line):
    text = StringIO.StringIO(line)
    domain = ""
    fields = text.split("\t")
    join country here
    return (domain, fields)

def joinCountry():
    pass

def myprint(text):
    print(text)

def formatLogLine(record):
    tokens = record.split("\t")
    url = tokens[4]
    m = re.search('/(.+?).raxcdn.com/', url)
    domain = m.group(1)
    bandwidth = tokens[6]
    region = countryMap.value[tokens[11]]
    return (domain, (domain, region, int(bandwidth)))

# create a combiner
def createCombiner((domain, region, bandwidth)):
    bw = {
        "EMEA": 0,
        "APAC": 0,
        "North America": 0,
        "South America": 0,
        "Japan": 0,
        "India": 0
    }
    bw[region] = bandwidth
    return (domain, bw)

# merge a value
def mergeValue((domain, bw), (domain1, region, bandwidth)):
    bw[region] = bw[region] + bandwidth
    return (domain, bw)

# merge two combiners: domain = domain1
def mergeCombiners((domain, bw1), (domain1, bw2)):
    for region in bw1:
        bw1[region] = bw1[region] + bw2[region]
    return (domain, bw1)

def process(master, input_container, output_container):
    import re

    # load broadcast variables
    countryMap = sc.broadcast(loadCountryMap())

    # load domain map: Ananta
    domain_map = load domains map
    # domainMap.foreach(myprint)

    # load logs
    logsRDD = sc.textFile("sample.log")
    # drop the header
    filteredRDD = logsRDD.filter(lambda x: x[0] != '#')
    # format the data
    formattedRDD = filteredRDD.map(formatLogLine, countryMap)
    # for each domain, calculate bandwidth and request count
    aggregatedLogs = formattedRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)
    # print the data
    aggregatedLogs.foreach(myprint)

    joinedLogs = aggregaredLogs.join()

    joinedLogs.saveAstextFile(output_contianer)

    sc.stop()
