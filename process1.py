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
    tokens.append(countryMap.value[tokens[11]])
    return (m.group(1), tokens)

def process(master, input_container, output_container):
    sc = SparkContext(master, "CDNBilling")

    # Anantha
    countryMap = sc.broadcast(sc.textFile(input_container + "/country_map.tsv"))
    domainMap = sc.textFile(input_container + "/domain_map.tsv")

    # load logs
    logsRDD = sc.textFile("sample.log")
    # drop the header
    filteredRDD = logsRDD.filter(lambda x: x[0] != '#')
    # format the data
    formattedRDD = filteredRDD.map(formatLogLine, countryMap)




    domainMap.foreach(myprint)
    # Obulpathi
    aggregatedLogs = domainLogs.reduceByKey(accumalator)

    joinedLogs = aggregaredLogs.join()

    joinedLogs.saveAstextFile(output_contianer)

    sc.stop()
