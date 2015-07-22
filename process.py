# Execution : $SPARK_HOME/bin/spark-submit --master local[4] process.py /home/nithya/spark-billing/sparkbilling /home/nithya/spark-billing/sparkbilling/output

from pyspark import SparkContext
import re
import time, datetime
import sys

countryMapDict = {}

def myprint(text):
    print(text)

def myprintlist(list):
    for elem in list:
        print elem

# Apache Extended Log Format
# date         time        ip          method  uri             status    bytes time_taken referer                                                                                              user_agent                                                                                                      cookie      Country
# 2015-01-31	00:01:34	92.63.87.3	GET     /abc.raxcdn.com/	301	      399	0	       "http://alea-laconica.gr/wp-admin/admin-ajax.php?action=kbslider_show_image&img=../wp-config.php"	"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.65 Safari/535.11"	"-"         "US"

def formatLogLine(record):
    try:
        tokens = record.split("\t")
        url = tokens[4]
        m = re.search('/(.+?).raxcdn.com/', url)
        domain = m.group(1)
        bandwidth = tokens[6]
        region = countryMapDict.get(tokens[11], 'None')
    except Exception as e :
        print "\n\nException:"
        print str(e.message) + "\n\nRecord:\n"
        print record.encode('utf-8')
        domain = "Invalid/Error"
        bandwidth = 0
        region = "None"
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
    count = 1
    bw[region] = bandwidth
    return (domain, bw, count)

# merge a value
def mergeValue((domain, bw, count), (domain1, region, bandwidth)):
    bw[region] = bw[region] + bandwidth
    count += 1
    return (domain, bw, count)

# merge two combiners: domain = domain1
def mergeCombiners((domain, bw1, count1), (domain1, bw2, count2)):
    for region in bw1:
        bw1[region] = bw1[region] + bw2[region]
    return (domain, bw1, count1 + count2)

def createCountryDict(list):
    country_list = []
    for item in list:
        tokens = item.split("\t")
        country_list.append((tokens[0],tokens[1]))
    return dict(country_list)

def get_time():
    timestamp = time.time()
    time_formatted = datetime.datetime.fromtimestamp(timestamp).strftime(
        '%Y-%m-%d %H:%M:%S')
    return time_formatted

def formatUnusedDomain((domain, value)):
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
    count = 0
    return (domain, (domain, bw, count), value)

def process(master, input_container, output_container):
    sc = SparkContext(master, "CDNBilling")

    # load broadcast variables
    countryMapRDD = sc.textFile(input_container + "/country_map.tsv")
    countryMapList = countryMapRDD.collect()
    countryMap = sc.broadcast(countryMapList)
    countryMapDict.update(createCountryDict(countryMapList))

    # load domainLogs
    domainsRawRDD = sc.textFile(input_container + "/domains_map.tsv")
    domainsRDD = domainsRawRDD.map(formatDomainsLine)

    # load logs
    logsRDD = sc.textFile(input_container + "/raxcdn_*")
    # drop the header
    filteredRDD = logsRDD.filter(lambda x: x[0] != '#')

    # format the data
    formattedRDD = filteredRDD.map(formatLogLine, countryMapDict)

    # Zero event domains
    domains_unused = domainsRDD.subtractByKey(formattedRDD)
    domains_unused_formatted = domains_unused.map(formatUnusedDomain)

    # for each domain, calculate bandwidth and request count
    aggregatedLogs = formattedRDD.combineByKey(createCombiner, mergeValue,
                                               mergeCombiners)

    # join the usage logs with domains map including zero events
    joinedLogs = aggregatedLogs.union(domains_unused_formatted)

    # save the output
    joinedLogs.saveAsTextFile(output_container + "/output-files")

    sc.stop()

def main(argv):
    if len(argv) == 2:
        input_container = argv[0]
        output_container = argv[1]
        f = open(output_container + "/time_taken.txt", 'w')
        f.write("Start time: " + get_time() + "\n")
        process("local", input_container, output_container)
        f.write("End time: " + get_time() + "\n")
        f.close()
    else:
        print ("Usage: spark-submit file.py input_container output_container")

if __name__ == "__main__":
    main(sys.argv[1:])
