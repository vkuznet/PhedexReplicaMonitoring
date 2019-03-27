#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       		: pbr.py
Author     		: Aurimas Repecka <aurimas.repecka AT gmail dot com>
Based On Work By   	: Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
    http://stackoverflow.com/questions/29936156/get-csv-to-spark-dataframe
    http://stackoverflow.com/questions/33878370/spark-dataframe-select-the-first-row-of-each-group
"""

# system modules
import os
import re
import sys
import json
import time
import argparse
import ConfigParser

from datetime import datetime as dt
from datetime import timedelta

# WMCore modules
try:
    # stopmAMQ API
    from WMCore.Services.StompAMQ.StompAMQ import StompAMQ
except ImportError:
    StompAMQ = None

# pyspark modules
from pyspark import SparkContext, StorageLevel
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType, BooleanType, LongType
from pyspark.sql.functions import udf, from_unixtime, date_format, regexp_extract, when, lit, lag, lead, coalesce, sum
from pyspark.sql import functions as F

# int number
INT_NUMBER = re.compile(r'(^[0-9-]$|^[0-9-][0-9]*$)')

# additional data needed for joins
# user group names
GROUP_CSV_PATH = os.path.join(os.environ.get('PBR_DATA', '/'), "phedex_groups.csv")
# node kinds
NODE_CSV_PATH = os.path.join(os.environ.get('PBR_DATA', '/'), "phedex_node_kinds.csv")
# elasticsearch node
CONFIG_PATH = os.path.join(os.environ.get('PBR_CONFIG', '/'), "pbr.cfg")

DELTA = "delta"
AVERAGEDAY = "avg-day"
LOGLEVELS = ["ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"]                      # supported spark log levels
AGGREGATIONS = ["sum", "count", "min", "max", "first", "last", "mean", "delta", "avg-day"]			# supported aggregation functions
GROUPKEYS = ["now", "dataset_name", "block_name", "node_name", "br_is_custiodial", "br_user_group",
            "data_tier", "acquisition_era", "node_kind", "now_sec", "node_tier", "campaign"]	    # supported group key values
GROUPRES = ["block_files", "block_bytes", "br_src_files", "br_src_bytes", "br_dest_files", 
            "br_dest_bytes", "br_node_files", "br_node_bytes", "br_xfer_files", "br_xfer_bytes"] 	# supported group result values

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        msg = "Input data file on HDFS, e.g. hdfs:///path/data/file"
        self.parser.add_argument("--fname", action="store",
            dest="fname", default="", help=msg)
        msg = 'Output file on HDFS, e.g. hdfs:///path/data/output.file'
        self.parser.add_argument("--fout", action="store",
            dest="fout", default="", help=msg)
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="Be verbose")
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="Be yarn")
        self.parser.add_argument("--basedir", action="store",
            dest="basedir", default="/project/awg/cms/phedex/block-replicas-snapshots/csv/", help="Base directory of snapshots")
        self.parser.add_argument("--fromdate", action="store",
            dest="fromdate", default="", help="Filter by start date")
        self.parser.add_argument("--todate", action="store",
            dest="todate", default="", help="Filter by end date")
        self.parser.add_argument("--keys", action="store",
            dest="keys", default="dataset_name, node_name", help="Names (csv) of group keys to use, supported keys: %s" % GROUPKEYS)
        self.parser.add_argument("--results", action="store",
            dest="results", default="block_files, block_bytes", help="Names (csv) of group results to use, supported results: %s" % GROUPRES)
        self.parser.add_argument("--aggregations", action="store",
            dest="aggregations", default="sum", help="Names (csv) of aggregation functions to use, supported aggregations: %s" % AGGREGATIONS)
        self.parser.add_argument("--order", action="store",
            dest="order", default="", help="Column names (csv) for ordering data")
        self.parser.add_argument("--asc", action="store",
            dest="asc", default="", help="1 or 0 (csv) for ordering columns (0-desc, 1-asc)")
        self.parser.add_argument("--header", action="store_true",
            dest="header", default=False, help="Print header in the first file of csv")
        self.parser.add_argument("--interval", action="store",
            dest="interval", default="1", help="Interval for delta operation in days")
        self.parser.add_argument("--filt", action="store",
            dest="filt", default="", help="Filtering field:regex in csv format")
        self.parser.add_argument("--collect", action="store_true",
            dest="collect", default=False, help="Collect before writing to file")
        self.parser.add_argument("--logs", action="store",
            dest="logs", default="INFO", help="Set log level to one of: " % LOGLEVELS)
        self.parser.add_argument("--es", action="store_true",
            dest="es", default=False, help="Writes result to elastic search")
        self.parser.add_argument("--esorigin", action="store",
            dest="esorigin", default="custom", help="Writes an data origin field to elastic search")
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        msg = "Send results via StompAMQ to a broker, provide broker credentials in JSON file"
        self.parser.add_argument("--amq", action="store",
            dest="amq", default="", help=msg)

def schema():
    """
    Provides schema (names, types, nullable) for csv snapshot

    :returns: StructType consisting StructField array
    """
    return StructType([StructField("now_sec", DoubleType(), True),
                     StructField("dataset_name", StringType(), True),
                     StructField("dataset_id", IntegerType(), True),
                     StructField("dataset_is_open", StringType(), True),
                     StructField("dataset_time_create", DoubleType(), True),
                     StructField("dataset_time_update", DoubleType(), True),
                     StructField("block_name", StringType(), True), 
                     StructField("block_id", IntegerType(), True),
                     StructField("block_files", IntegerType(), True),
                     StructField("block_bytes", DoubleType(), True),
                     StructField("block_is_open", StringType(), True),
                     StructField("block_time_create", DoubleType(), True),
                     StructField("block_time_update", DoubleType(), True),
                     StructField("node_name", StringType(), True),
                     StructField("node_id", IntegerType(), True),
                     StructField("br_is_active", StringType(), True),
                     StructField("br_src_files", LongType(), True),
                     StructField("br_src_bytes", LongType(), True),
                     StructField("br_dest_files", LongType(), True),
                     StructField("br_dest_bytes", LongType(), True),
                     StructField("br_node_files", LongType(), True),
                     StructField("br_node_bytes", LongType(), True),
                     StructField("br_xfer_files", LongType(), True),
                     StructField("br_xfer_bytes", LongType(), True),
                     StructField("br_is_custodial", StringType(), True),
                     StructField("br_user_group_id", IntegerType(), True),
                     StructField("replica_time_create", DoubleType(), True),
                     StructField("replica_time_updater", DoubleType(), True)])


def toStringVal(item):
    """
    Converts element or iterable item to string representation

    :param item: single element or collection of elements
    :returns: element string representation
    """
    return ','.join(str(i) for i in item) if hasattr(item, '__iter__') else str(item)


def getJoinDic():   
    """
    Gets group and node dictionaries needed for joins 

    :returns: group and node dictionaries
    """
    groupdic = {None : "null"}
    with open(GROUP_CSV_PATH) as fg:
        for line in fg.read().splitlines():
            (gid, gname) = line.split(',')
            groupdic[int(gid)] = gname

    nodedic = {None : "null"}
    with open(NODE_CSV_PATH) as fn:
        for line in fn.read().splitlines():
            data = line.split(',')
            nodedic[int(data[0])] = data[2] 

    return groupdic, nodedic  


def getFileList(basedir, fromdate, todate):
    """
    Finds snapshots in given directory by interval dates

    :param basedir: directory where snapshots are held
    :param fromdate: date from which snapshots are filtered
    :param todate: date until which snapshots are filtered
    :returns: array of filtered snapshots paths
    :raises ValueError: if unparsable date format
    """
    dirs = os.popen("hadoop fs -ls %s | sed '1d;s/  */ /g' | cut -d\  -f8" % basedir).read().splitlines()
    # if files are not in hdfs --> dirs = os.listdir(basedir)

    o_fromdate = fromdate
    o_todate = todate
    try:
        fromdate = dt.strptime(fromdate, "%Y-%m-%d")
        todate = dt.strptime(todate, "%Y-%m-%d")
    except ValueError as err:
        raise ValueError("Unparsable date parameters. Date should be specified in form: YYYY-mm-dd")		
 		
    pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    dirdate_dic = {}
    from_match = 0
    to_match = 0
    for idir in dirs:
        if  idir.find(o_fromdate) != -1:
            from_match = 1
        if  idir.find(o_todate) != -1:
            to_match = 1
        matching = pattern.search(idir)
        if matching:
            dirdate_dic[idir] = dt.strptime(matching.group(1), "%Y-%m-%d")

    if  not from_match:
        raise Exception("Unable to find fromdate=%s are on HDFS %s" % (o_fromdate, basedir))
    if  not to_match:
        raise Exception("Unable to find todate=%s are on HDFS %s" % (o_todate, basedir))

    # if files are not in hdfs --> return [ basedir + k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]
    return [k for k, v in dirdate_dic.items() if v >= fromdate and v <= todate]		


def validateAggregationParams(keys, res, agg, order, filt):
    """
    Validates aggregation parameters and form error message

    :param keys: list of aggregation key fields (from GROUPKEYS)
    :param res: list of aggregation result fields (from GROUPRES)
    :param agg: list of aggregation type (from AGGREGATIONS)
    :param order: list of ordering fields (from GROUPKEYS + GROUPRES)
    :param filt: filtering field (from GROUPKEYS)
    :raises NotImplementedError: if any of parameters are not in provided lists
    """
    unsup_keys = set(keys).difference(set(GROUPKEYS)) 
    unsup_res = set(res).difference(set(GROUPRES))
    unsup_agg = set(agg).difference(set(AGGREGATIONS))
    unsup_ord = set(order).difference(set(keys + res)) if order != [''] else None
    unsup_filt = set(filt).difference(set(GROUPKEYS)) if filt != [''] else None

    msg = ""
    if unsup_keys:
        msg += 'Group key(s) = "%s" are not supported. ' % toStringVal(unsup_keys)
    if unsup_res:
        msg += 'Group result(s) = "%s" are not supported. ' % toStringVal(unsup_res)
    if unsup_agg:
        msg += 'Aggregation function(s) = "%s" are not supported. ' % toStringVal(unsup_agg)
    if unsup_ord:
        msg += 'Order key(s) = "%s" are not available. ' % toStringVal(unsup_ord)
    if unsup_filt:
        msg += 'Filtering field = "%s" is not available' % toStringVal(unsup_filt)
    if msg:
        raise NotImplementedError(msg)


def validateLogLevel(log_level):
    """
    Validates user specified spark log level

    :param log_level: string of log level
    :raises ValueError: if log level is not in LOGLEVELS list
    """
    if log_level not in LOGLEVELS:
        raise ValueError("Specified log level = %s not available" % log_level)


def validateEsParams(node, port, resource):
    """
    Validates user specified Elasticsearch parameters

    :param node: string representation of elasticsearch node
    :param port: string representation of elasticearch port
    :param resource string representation of elasticsearch index/type
    :raises ValueError: if Elasticsearch node or port were not specified or index/type was not in correct form
    """
    if not node:
        raise ValueError("Elasticsearch node was not specified")
    if not port:
        raise ValueError("Elasticsearch port was not specified")
    if len(resource.split('/')) != 2:
        raise ValueError("Elasticsearch index/type was not provided in the correct form")

def defDates(fromdate, todate):
    """
    Check if dates are specified and returns default values

    :param fromdate: interval beggining date
    :param todate: interval end date
    :returns: tuple of from and to dates
    """
    if not fromdate or not todate:
        fromdate = dt.strftime(dt.now(), "%Y-%m-%d")
        todate = dt.strftime(dt.now(), "%Y-%m-%d")
    return fromdate, todate


def zipResultAgg(res, agg):
    """
    Zips results fields and aggregation types into one dictionary

    :param res: list of aggregation result fields
    :param agg: list of aggregation types
    :returns: dictionary with result fields and aggregation types
    """
    return dict(zip(res, agg)) if len(res) == len(agg) else dict(zip(res, agg * len(res)))


def formOrdAsc(order, asc, resAgg_dic):
    """
    Forms ordering fields and ordering values arrays according aggregation functions

    :param order: list of ordering fields
    :param asc: list of booleans ascending
    :returns: order fields and ascending arrays
    """
    asc = map(int, asc) if len(order) == len(asc) else [1] * len(order)
    orderN = [resAgg_dic[orde] + "(" + orde + ")" if orde in resAgg_dic.keys() else orde for orde in order] 
    return orderN, asc


def unionAll(dfs):
    """
    Unions snapshots in one dataframe	

    :param item: list of dataframes
    :returns: union of dataframes
    """
    return reduce(DataFrame.unionAll, dfs)		


def formFileHeader(fout):
    """
    Forms output file header with date, time

    :param fout: base dirctory of output files
    :returns: string representation of output file
    """

    return  fout + "/" + dt.strftime(dt.now(), "%Y-%m-%d_%Hh%Mm%Ss")

def date_int(dateval, iformat='%Y-%m-%d'):
    "Convert given date value into integer"
    if  isinstance(dateval, str):
        return int(dateval.replace('-', ''))
    if  isinstance(dateval, dt):
        return date_int(dt.strftime(dateval, iformat))
    raise NotImplementedError("date value %s of type %s is not supported" \
            % (dateval, type(dateval)))

def generateDateDict(fromdate, todate, interval, iformat='%Y-%m-%d'):
    if  fromdate == todate:
        return {fromdate: 1}
    dates = []
    if  isinstance(interval, str):
        if  interval.endswith('m') or interval.endswith('month') or interval.endswith('months'):
            cdate = fromdate
            while True:
                edate = add_one_month(dt.strptime(cdate, iformat))
                if  date_int(edate, iformat) > date_int(todate):
                    break
                dates.append([cdate, dt.strftime(edate-timedelta(days=1), iformat)])
                cdate = dt.strftime(edate, iformat)
        elif interval.endswith('w') or interval.endswith('week') or interval.endswith('weeks'):
            interval = 7*int(interval.split('w')[0])
        elif interval.endswith('d') or interval.endswith('day') or interval.endswith('days'):
            interval = int(interval.split('d')[0])
        else:
            interval = int(interval)
    if  isinstance(interval, int):
        cdate = fromdate
        while True:
            edate = dt.strptime(cdate, iformat)+timedelta(days=interval)
            if  date_int(edate, iformat) > date_int(todate):
                break
            dates.append([cdate, dt.strftime(edate-timedelta(days=1), iformat)])
            cdate = dt.strftime(edate, iformat)
    odict = {}
    for idx, pair in enumerate(dates):
        cdate = pair[0]
        edate = pair[1]
        while True:
            if  date_int(cdate) > date_int(edate):
                break
            odict[cdate] = idx+1
            mdate = dt.strptime(cdate, iformat) + timedelta(days=1)
            cdate = dt.strftime(mdate, iformat)
    return odict

def generateBoundDict(datedic):
    """
    Generate dictionary with dates and its interval boundaries (start, end)

    :param item: date dictionary with date and interval pairs
    :returns: dictionary with dates and start and end of interval
    """
    boundic = {}
    intervals = set(datedic.values())

    for interval in intervals:
        values = [k for k, v in datedic.items() if v == interval]
        boundic[interval] = [min(values), max(values)]
		
    return boundic

def add_one_month(dt0):
    """Return a `datetime.date` or `datetime.datetime` (as given) that is
    one month earlier.
    
    Note that the resultant day of the month might change if the following
    month has fewer days:
    
        >>> add_one_month(datetime.date(2010, 1, 31))
        datetime.date(2010, 2, 28)

    Reference: http://bit.ly/2gCGAyX
    """
    dt1 = dt0.replace(day=1)
    dt2 = dt1 + timedelta(days=32)
    dt3 = dt2.replace(day=1)
    return dt3

def subtract_one_month(dt0):
    """Return a `datetime.date` or `datetime.datetime` (as given) that is
    one month later.
    
    Note that the resultant day of the month might change if the following
    month has fewer days:
    
        >>> subtract_one_month(datetime.date(2010, 3, 31))
        datetime.date(2010, 2, 28)

    Reference: http://bit.ly/2gCGAyX
    """
    dt1 = dt0.replace(day=1)
    dt2 = dt1 - timedelta(days=1)
    dt3 = dt2.replace(day=1)
    return dt3

def to_csv(data):
    "Convert data rows into CSV format"
    return ','.join(str(d) for d in data)

def credentials(fname=None):
    "Read credentials from PBR_BROKER environment"
    if  not fname:
        fname = os.environ.get('PBR_BROKER', '')
    if  not os.path.isfile(fname):
        return {}
    with open(fname, 'r') as istream:
        data = json.load(istream)
    return data

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    config = ConfigParser.ConfigParser()
    config.read(CONFIG_PATH)
    if  opts.es:
        esnode = config.get('ElasticSearch','node')
        esport = config.get('ElasticSearch','port')
        esresource = config.get('ElasticSearch','resource')

    # setup spark/sql context to be used for communication with HDFS
    sc = SparkContext(appName="phedex_br")

    # setting spark log level
    logs = opts.logs.upper()
    validateLogLevel(logs)
    sc.setLogLevel(logs)

    # setting up spark sql variables
    sqlContext = HiveContext(sc)
    schema_def = schema()

    # read given file(s) into RDD
    if opts.fname:
        pdf = sqlContext.read.format('com.databricks.spark.csv')\
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(opts.fname, schema = schema_def)
    elif opts.basedir:
        fromdate, todate = defDates(opts.fromdate, opts.todate)
        datedic = generateDateDict(fromdate, todate, opts.interval)
        boundic = generateBoundDict(datedic)
        max_interval = max(datedic.values())
        lastdate = sorted(datedic.keys())[-1]
        files = getFileList(opts.basedir, fromdate, lastdate)
        msg = "Between dates %s and %s found %d directories" % (fromdate, lastdate, len(files))
        print msg

        if not files:
            return
        pdf = unionAll([sqlContext.read.format('com.databricks.spark.csv')
                        .options(treatEmptyValuesAsNulls='true', nullValue='null')\
                        .load(file_path, schema = schema_def) \
                        for file_path in files])
    else:
        raise ValueError("File or directory not specified. Specify fname or basedir parameters.")

    # parsing additional data (to given data adding: group name, node kind, acquisition era, data tier, now date)
    groupdic, nodedic = getJoinDic()
    acquisition_era_reg = r"^/[^/]*/([^/^-]*)-[^/]*/[^/]*$"	
    data_tier_reg = r"^/[^/]*/[^/^-]*-[^/]*/([^/]*)$"
    node_tier_reg = r"^(.{2})"
    campaign_reg = r"^/[^/]*/([^/]*)-[^/]*/[^/]*$"
    groupf = udf(lambda x: groupdic.get(x, ''), StringType())
    nodef = udf(lambda x: nodedic.get(x, ''), StringType())
    regexudf = udf(lambda x, y: bool(regexp_extract(x, y, 1)), BooleanType())

    ndf = pdf.withColumn("br_user_group", groupf(pdf.br_user_group_id)) \
         .withColumn("node_kind", nodef(pdf.node_id)) \
         .withColumn("now", from_unixtime(pdf.now_sec, "yyyy-MM-dd")) \
         .withColumn("acquisition_era", when(regexp_extract(pdf.dataset_name, acquisition_era_reg, 1) == "",\
                     lit("null")).otherwise(regexp_extract(pdf.dataset_name, acquisition_era_reg, 1))) \
         .withColumn("data_tier", when(regexp_extract(pdf.dataset_name, data_tier_reg, 1) == "",\
                     lit("null")).otherwise(regexp_extract(pdf.dataset_name, data_tier_reg, 1)))\
         .withColumn("node_tier", when(regexp_extract(pdf.node_name, node_tier_reg, 1) == "",\
                     lit("null")).otherwise(regexp_extract(pdf.node_name, node_tier_reg, 1)))\
         .withColumn("campaign", when(regexp_extract(pdf.dataset_name, campaign_reg, 1) == "",\
                     lit("null")).otherwise(regexp_extract(pdf.dataset_name, campaign_reg, 1)))

    # print dataframe schema
    if opts.verbose:
        print("pdf data type", type(ndf))
        ndf.printSchema()
#         print("ndf total:", ndf.count())
#         for row in ndf.head(10):
#             print(row)

    # process aggregation parameters
    keys = [key.lower().strip() for key in opts.keys.split(',')]
    results = [result.lower().strip() for result in opts.results.split(',')]
    aggregations = [agg.strip() for agg in opts.aggregations.split(',')]
    order = [orde.strip() for orde in opts.order.split(',')] if opts.order else []
    asc = [asce.strip() for asce in opts.asc.split(',')] if opts.order else []
    filtc = [fil.split(':')[0] for fil in opts.filt.split(',')] if opts.filt else []
    filtv = [fil.split(':')[1] for fil in opts.filt.split(',')] if opts.filt else []
    isavgday = (AVERAGEDAY in aggregations)

    validateAggregationParams(keys, results, aggregations, order, filtc)

    # filtering data by regex
    for index, val in enumerate(filtc):
        ndf = ndf.filter(regexp_extract(getattr(ndf, val), filtv[index], 0) != "")

    # if delta aggregation is used
    if DELTA in aggregations:
        if len(results) != 1:
            raise ValueError("Delta aggregation can have only 1 result field")
        result = results[0]

        #1 for all dates generate interval group dictionary
        interval_group = udf(lambda x: datedic[x], IntegerType())
        interval_start = udf(lambda x: boundic[x][0], StringType())		
        interval_end = udf(lambda x: boundic[x][1], StringType())

        #2 group data by block, node, interval and last result in the interval
        ndf = ndf.select(ndf.block_name, ndf.node_name, ndf.now, getattr(ndf, result))
        idf = ndf.withColumn("interval_group", interval_group(ndf.now))
        win = Window.partitionBy(idf.block_name, idf.node_name, idf.interval_group).orderBy(idf.now.desc())	
        idf = idf.withColumn("row_number", F.rowNumber().over(win))
        rdf = idf.where((idf.row_number == 1) & (idf.interval_group != 0))\
                 .withColumn(result, when(idf.now == interval_end(idf.interval_group), getattr(idf, result)).otherwise(lit(0)))
        rdf = rdf.select(rdf.block_name, rdf.node_name, rdf.interval_group, getattr(rdf, result))

        #3 create intervals that not exist but has minus delta
        win = Window.partitionBy(idf.block_name, idf.node_name).orderBy(idf.interval_group)
        adf = rdf.withColumn("interval_group_aft", lead(rdf.interval_group, 1, 0).over(win))
        hdf = adf.filter(((adf.interval_group + 1) != adf.interval_group_aft) & (adf.interval_group != max_interval))\
                 .withColumn("interval_group", adf.interval_group + 1)\
                 .withColumn(result, lit(0))\
                 .drop(adf.interval_group_aft)

        #4 join data frames
        idf = rdf.unionAll(hdf)
		
        #3 join every interval with previous interval
        win = Window.partitionBy(idf.block_name, idf.node_name).orderBy(idf.interval_group)
        fdf = idf.withColumn("delta", getattr(idf, result) - lag(getattr(idf, result), 1, 0).over(win))

        #5 calculate delta_plus and delta_minus columns and aggregate by date and node
        ddf =fdf.withColumn("delta_plus", when(fdf.delta > 0, fdf.delta).otherwise(0)) \
                .withColumn("delta_minus", when(fdf.delta < 0, fdf.delta).otherwise(0))

        aggres = ddf.groupBy(ddf.node_name, ddf.interval_group).agg(sum(ddf.delta_plus).alias("delta_plus"),\
                                                                    sum(ddf.delta_minus).alias("delta_minus"))

        aggres = aggres.select(aggres.node_name, interval_end(aggres.interval_group).alias("date"), aggres.delta_plus, aggres.delta_minus)
    
    else:
        # clean-up unnecessary dataframe and columns
        pdf.unpersist()

        if isavgday:
            datescount = ndf.select(ndf.now).distinct().count()
            aggregations = ["sum" if aggregation == "avg-day" else aggregation for aggregation in aggregations]
        
        resAgg_dic = zipResultAgg(results, aggregations)
        order, asc = formOrdAsc(order, asc, resAgg_dic)

        # perform aggregation
        if order:
            aggres = ndf.groupBy(keys).agg(resAgg_dic).orderBy(order, ascending=asc)
        else:
            aggres = ndf.groupBy(keys).agg(resAgg_dic)
        ndf.unpersist()

        # if average day then divide by dates count
        if isavgday:
            resfields = [resAgg_dic[result] + "(" + result + ")" for result in results]
            for field in resfields:
                aggres = aggres.withColumn(field, getattr(aggres, field)/datescount)

    # output results
    if opts.fout:
        is_header = str(opts.header).lower()
        fout_header = formFileHeader(opts.fout)

        if opts.collect:
            fout_header = fout_header + ".json"
            aggres = aggres.toJSON().collect()
            with open(fout_header, 'w+') as f:
                f.write('[')
                f.write(",".join(aggres))
                f.write(']')
        else:
#             lines = aggres.map(to_csv)
#             lines.saveAsTextFile(opts.fout)
            aggres.write.csv(opts.fout)
        
        if opts.es:
            validateEsParams(esnode, esport, esresource)
            aggres = aggres.withColumn("origin", lit(opts.esorigin))
            aggres.repartition(1).write.format("org.elasticsearch.spark.sql").option("es.nodes", esnode)\
                                                                                 .option("es.port", esport)\
                                                                                 .option("es.resource", esresource)\
                                                                                 .save(mode="append")

    else:
        for row in aggres.head(10):
            print(row)

    if opts.amq:
        creds = credentials(opts.amq)
        host, port = creds['host_and_ports'].split(':')
        port = int(port)
        aggres = aggres.toJSON().collect()
        if  creds and StompAMQ:
            print("### Send %s docs via StompAMQ" % len(aggres))
            amq = StompAMQ(creds['username'], creds['password'], \
                creds['producer'], creds['topic'], [(host, port)])
            data = []
            for doc in aggres:
                doc = json.loads(doc)
                hid = doc.get("hash", 1)
                data.append(amq.make_notification(doc, hid))
            results = amq.send(data)
            print("### results sent by AMQ", len(results))

if __name__ == '__main__':
    main()

