def loadLogs(line):
    text = StringIO.StringIO(line)
    domain = ""
    fields = text.split("\t")
    join coutry here
    return (domain, fields)
-------------------------------------------------------
def joinCountry():
    pass

def process(master, input_container, output_container):
    sc = SparkContext(master, "CDNBilling")

    country_map = load as broadcast variables
    domain_map = load domains map

    logs = sc.TextFile(gradesFileName)
    domainLogs = logs.map(loadLogs)
    --------------------------------------------------
    aggregatedLogs = domainLogs.reduceByKey(accumalator)

    joinedLogs = aggregaredLogs.join()

    joinedLogs.saveAstextFile(output_contianer)

    sc.stop()
