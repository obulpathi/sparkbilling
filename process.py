# Execution : $SPARK_HOME/bin/spark-submit --master local[4] process.py \
# --input_container /home/nithya/spark-billing/sparkbilling/sample \
# --output_container /home/nithya/spark-billing/sparkbilling/output \
# --start_date 2015-03-30T12:00:00Z --end_date 2015-03-31T12:00:00Z


from pyspark import SparkContext
import argparse
import datetime
import re
import time
import sys

countryMapDict = {}
dateDict = {}


def myprint(text):
    print(text)


def myprintlist(list):
    for elem in list:
        print elem


def formatLogLine(record):
    try:
        tokens = record.split("\t")
        url = tokens[4]
        m = re.search('/(.+?).raxcdn.com/', url)
        domain = m.group(1)
        bandwidth = tokens[6]
        region = countryMapDict.get(tokens[11], 'None')
    except Exception as e:
        print "\n\nException:"
        print str(e.message) + "\n\nRecord:\n"
        print record.encode('utf-8')
        domain = "Invalid/Error"
        bandwidth = 0
        region = "None"
    return (domain, (region, int(bandwidth)))


def formatDomainsLine(record):
    tokens = record.split("\t")
    return (tokens[0], (tokens[1], tokens[3], tokens[2]))


# create a combiner
def createCombiner((region, bandwidth)):
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
    request_count = {
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
    request_count[region] = 1
    return (bw, request_count)


# merge a value
def mergeValue((bw, request_count), (region, bandwidth)):
    bw[region] = bw[region] + bandwidth
    request_count[region] += 1
    return (bw, request_count)


# merge two combiners: domain = domain1
def mergeCombiners((bw1, req_count1), (bw2, req_count2)):
    for region in bw1:
        bw1[region] = bw1[region] + bw2[region]
        req_count1[region] = req_count1[region] + req_count2[region]
    return (bw1, req_count1)


def createCountryDict(list):
    country_list = []
    for item in list:
        tokens = item.split("\t")
        country_list.append((tokens[0], tokens[1]))
    return dict(country_list)


def get_time():
    timestamp = time.time()
    time_formatted = datetime.datetime.fromtimestamp(timestamp).strftime(
        '%Y-%m-%d %H:%M:%S')
    return time_formatted


def filterByDate(tuple):
    tokens = tuple.split("\t")
    date = datetime.datetime.strptime(tokens[0] + ":" + tokens[1],
                                      '%Y-%m-%d:%X')
    if dateDict['start_date'] <= date <= dateDict['end_date']:
        return tuple


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
    request_count = {
        "EMEA": 0,
        "APAC": 0,
        "North America": 0,
        "South America": 0,
        "Japan": 0,
        "India": 0,
        "Australia": 0,
        "None": 0
    }
    return (domain, (domain, bw, request_count), value)


def process(master, input_container, output_container):
    sc = SparkContext(master, "CDNBilling")

    # load broadcast variables
    countryMapRDD = sc.textFile(input_container + "/country_map.tsv")
    countryMapList = countryMapRDD.collect()
    sc.broadcast(countryMapList)
    countryMapDict.update(createCountryDict(countryMapList))

    # load domainLogs
    domainsRawRDD = sc.textFile(input_container + "/domains_map.tsv")
    domainsRDD = domainsRawRDD.map(formatDomainsLine)

    # load logs
    logsRDD = sc.textFile(input_container + "/raxcdn_*.gz")
    # drop the header
    actual_log_lines = logsRDD.filter(lambda x: x[0] != '#')

    # filter by date
    filteredRDD = actual_log_lines.filter(filterByDate)

    # format the data
    formattedRDD = filteredRDD.map(formatLogLine, countryMapDict)

    # Zero event domains
    domains_unused = domainsRDD.subtractByKey(formattedRDD)
    domains_unused_formatted = domains_unused.map(formatUnusedDomain)

    # for each domain, calculate bandwidth and request count
    aggregatedLogs = formattedRDD.combineByKey(createCombiner, mergeValue,
                                               mergeCombiners)

    # add type of domain, project-ID, service-ID
    joinedWithDomainDetails = aggregatedLogs.join(domainsRDD)

    # join the usage logs with domains map including zero events
    joinedLogs = joinedWithDomainDetails.union(domains_unused_formatted)

    # save the output
    joinedLogs.saveAsTextFile(output_container + "/output-files")

    sc.stop()


def main(argv):
    parser = argparse.ArgumentParser()
    input_args = parser.add_argument_group('required named arguments')
    input_args.add_argument("--input_container", "-i",
                            help="Where the log files, domain map and "
                                 "country map are stored.",
                            required=True)
    input_args.add_argument("--output_container", "-o",
                            help="Where you want the output files to be "
                                 "stored.",
                            required=True)
    input_args.add_argument("--start_date",
                            help="The date starting from which logs should "
                                 "be processed. Example "
                                 "value:2015-12-01T14:00:00Z",
                            required=True)
    input_args.add_argument("--end_date",
                            help="The date until which logs should be "
                                 "processed. Example "
                                 "value:2015-12-02T14:00:00Z",
                            required=True)
    args = parser.parse_args()

    input_container = args.input_container
    output_container = args.output_container
    dateDict['start_date'] = datetime.datetime.strptime(args.start_date,
                                                        '%Y-%m-%dT%XZ')
    dateDict['end_date'] = datetime.datetime.strptime(args.end_date,
                                                      '%Y-%m-%dT%XZ')

    f = open(output_container + "/time_taken.txt", 'w')
    f.write("Start time: " + get_time() + "\n")
    process("local", input_container, output_container)
    f.write("End time: " + get_time() + "\n")
    f.close()


if __name__ == "__main__":
    main(sys.argv[1:])
