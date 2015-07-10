def loadLogs(line):
    text = StringIO.StringIO(line)
    domain = ""
    fields = text.split("\t")
    join coutry here
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
    sc = sc.SparkContext(master, "CDNBilling")
    countryMap = sc.broadcast(loadCountryMap())

    # Anantha
    country_map = load as broadcast variables
    domain_map = load domains map

    # load logs
    logsRDD = sc.textFile("sample.log")
    # drop the header
    filteredRDD = logsRDD.filter(lambda x: x[0] != '#')
    # format the data
    formattedRDD = filteredRDD.map(formatLogLine, countryMap)




    domainsRDD.foreach(myprint)
    # Obulpathi
    aggregatedLogs = domainLogs.reduceByKey(accumalator)

    joinedLogs = aggregaredLogs.join()

    joinedLogs.saveAstextFile(output_contianer)

    sc.stop()
