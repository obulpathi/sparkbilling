def myprint(text):
    print(text)

def formatLogLine(record):
    tokens = record.split("\t")
    url = tokens[4]
    m = re.search('/(.+?).raxcdn.com/', url)
    domain = m.group(1)
    bandwidth = tokens[6]
    region = countryMap.value.get(tokens[11], 'None')
    return (domain, (domain, region, int(bandwidth)))

def formatDomainsLine(record):
    tokens = record.split("\t")
    return (tokens[0], (tokens[1], tokens[3], tokens[2]))

# create a combiner
def createCombiner((domain, region, bandwidth)):
    bw = {
        "EMEA": 0,
        "APAC": 0,
        "North America": 0,
        "South America": 0,
        "Japan": 0,
        "India": 0,
        "Australia": 0,
        "None": 0
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

def process():
    import re

    # load broadcast variables
    countryMap = sc.broadcast(loadCountryMap())

    # load domainLogs
    domainsRawRDD = sc.textFile("domains_map.tsv")
    domainsRDD = domainsRawRDD.map(formatDomainsLine)
    # join the two above lines into one using wholeFilesRDD?

    # load logs
    logsRDD = sc.textFile("raxcdn*.gz")
    # drop the header
    filteredRDD = logsRDD.filter(lambda x: x[0] != '#')
    # the above two steps can be optimized into a single step using
    # wholeFilesRDD?
    # format the data
    formattedRDD = filteredRDD.map(formatLogLine, countryMap)
    # for each domain, calculate bandwidth and request count
    aggregatedLogs = formattedRDD.combineByKey(createCombiner, mergeValue, mergeCombiners)

    # join the usage logs with domains map
    joinedLogs = aggregatedLogs.join(domainsRDD)
    # save the output
    joinedLogs.saveAsTextFile("output")

    sc.stop()

if __name__ == "__main__":
    process()
