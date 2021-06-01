import requests
import gzip
import argparse
import csv
import datetime
import pytz
import math
from pytz import timezone

from influxdb import InfluxDBClient

epoch_naive = datetime.datetime.utcfromtimestamp(0)
epoch = timezone('UTC').localize(epoch_naive)

nan = set(['NaN'])

def unix_time_millis(dt):
    return int((dt - epoch).total_seconds() * 1000)

def convert_seconds_to_dt(s):
    return datetime.datetime.fromtimestamp(s, tz=pytz.utc)

def convert_microseconds_to_dt(us):
    return datetime.datetime.fromtimestamp(us/1000000.0, tz=pytz.utc)

def convert_milliseconds_to_dt(ms):
    return datetime.datetime.fromtimestamp(ms/1000.0, tz=pytz.utc)

##
## Check if data type of field is float
##
def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False    

def isbool(value):
    try:
        return value.lower() in ('true', 'false')
    except:
        return False

def str2bool(value):
    return value.lower() == 'true'

##
## Check if data type of field is int
##
def isinteger(value):
    try:
        if(float(value).is_integer()):
            return True
        else:
            return False
    except:
        return False

def isnan(value):
    return value in nan

def digits(val):
    return int(math.log10(val)) + 1

def ts_to_dt(ts):
    # Convert to datetime
    d = digits(ts)
    if d < 13:
        return convert_seconds_to_dt(ts)
    elif d < 16:
        return convert_milliseconds_to_dt(ts)
    else:
        return convert_microseconds_to_dt(ts)

def timefield_to_timestamp(tf, timeformat, datatimezone):
    # If timestamp field is integer
    if isinteger(tf):
       datetime_naive = ts_to_dt(int(tf))
    # Otherwise as string
    else:
        datetime_naive = datetime.datetime.strptime(tf, timeformat)    

    if datetime_naive.tzinfo is None:
        datetime_local = timezone(datatimezone).localize(datetime_naive)
    else:
        datetime_local = datetime_naive 

    timestamp = unix_time_millis(datetime_local) * 1000000  # in nanoseconds

    return timestamp, datetime_local

def avoid_nan_fields(fields=None, nan_strs=["NaN"]):
    if fields:
        return True
    else:
        return False

# Support Database&CSV colume mapping
def field_mapping(fieldcolumns):
    field_columes = []

    for cm in fieldcolumns:
        m = cm.split(':')
        if len(m) > 1:
            field_columes.append((m[0], m[1]))
        else:
            field_columes.append((m[0], m[0]))

    # list of (csv_colume, database_colume)
    return field_columes

# Support:
# 1. tags from colume mapping
# 2. tags from specificed inputs
def tags_mapping(tagdefines):
    tagcolumns = []
    tagvalues = []

    for cm in tagdefines:
        # 1. tags defined in colume value(use :)
        m = cm.split(':')
        if len(m) > 1:
            tagcolumns.append((m[0], m[1]))
        # 2. tags defined in static value(use =)
        else:
            m = cm.split('=')
            if len(m) > 1:
                tagvalues.append((m[0], m[1]))
            else:
                tagcolumns.append((m[0], m[0]))

    # tagcolumns: list of (csv_colume, database_tag_key)
    # tagvalues: list of (database_tag_key, tag_value)
    return tagcolumns, tagvalues


def loadCsv(inputfilename, servername, user, password, dbname, metric, 
            timecolumn, timeformat, tagcolumns, fieldcolumns, usegzip,
            delimiter, batchsize, create, datatimezone, usessl, ignorenancolumns):

    host = servername[0:servername.rfind(':')]
    port = int(servername[servername.rfind(':')+1:])
    client = InfluxDBClient(host, port, user, password, dbname, ssl=usessl)
    tagvalues = []
    ignore_nan = False

    if(create == True):
        print('Deleting database %s'%dbname)
        client.drop_database(dbname)
        print('Creating database %s'%dbname)
        client.create_database(dbname)

    client.switch_user(user, password)

    # format tags and fields
    if tagcolumns:
        tagcolumns, tagvalues = tags_mapping(tagcolumns.split(','))

    if fieldcolumns:
        fieldcolumns = field_mapping(fieldcolumns.split(','))

    if ignorenancolumns:
        ignorenancolumns = field_mapping(ignorenancolumns.split(','))
        ignore_nan = len(ignorenancolumns) > 0

    # open csv
    datapoints = []
    inputfile = open(inputfilename, 'r')
    count = 0
    with inputfile as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        for row in reader:

            # Ignore nan
            if ignore_nan:
                jump_to_next = False
                for nancol in ignorenancolumns:
                    if isnan(row[nancol[0]]):
                        jump_to_next = True
                        break

                if jump_to_next:
                    continue

            # Parse timestamp
            timestamp, datetime_local = timefield_to_timestamp(row[timecolumn], timeformat, datatimezone)

            # assemble tags
            tags = {}
            for t in tagcolumns:
                v = 0
                if t in row:
                    v = row[t[0]]
                tags[t[1]] = v

            for t in tagvalues:
                tags[t[0]] = t[1]

            # assemble fields
            fields = {}
            for f in fieldcolumns:
                v = 0
                if f[0] in row:
                    if (isfloat(row[f[0]])):
                        v = float(row[f[0]])
                    elif (isbool(row[f[0]])):
                        v = str2bool(row[f[0]])
                    else:
                        v = row[f[0]]
                fields[f[1]] = v


            point = {"measurement": metric, "time": timestamp, "fields": fields, "tags": tags}

            datapoints.append(point)
            count+=1
            
            if len(datapoints) % batchsize == 0:
                print('Read %d lines'%count)
                print('Inserting %d datapoints...'%(len(datapoints)))
                response = client.write_points(datapoints)

                if not response:
                    print('Problem inserting points, exiting...')
                    exit(1)

                print("Wrote %d points, up to %s, response: %s" % (len(datapoints), datetime_local, response))

                datapoints = []
            

    # write rest
    if len(datapoints) > 0:
        print('Read %d lines'%count)
        print('Inserting %d datapoints...'%(len(datapoints)))
        response = client.write_points(datapoints)

        if response == False:
            print('Problem inserting points, exiting...')
            exit(1)

        print("Wrote %d, response: %s" % (len(datapoints), response))

    print('Done')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Csv to influxdb.')

    parser.add_argument('-i', '--input', nargs='?', required=True,
                        help='Input csv file.')

    parser.add_argument('-d', '--delimiter', nargs='?', required=False, default=',',
                        help='Csv delimiter. Default: \',\'.')

    parser.add_argument('-s', '--server', nargs='?', default='localhost:8086',
                        help='Server address. Default: localhost:8086')

    parser.add_argument('--ssl', action='store_true', default=False,
                        help='Use HTTPS instead of HTTP.')

    parser.add_argument('-u', '--user', nargs='?', default='root',
                        help='User name.')

    parser.add_argument('-p', '--password', nargs='?', default='root',
                        help='Password.')

    parser.add_argument('--dbname', nargs='?', required=True,
                        help='Database name.')

    parser.add_argument('--create', action='store_true', default=False,
                        help='Drop database and create a new one.')

    parser.add_argument('-m', '--metricname', nargs='?', default='value',
                        help='Metric column name. Default: value')

    parser.add_argument('-tc', '--timecolumn', nargs='?', default='timestamp',
                        help='Timestamp column name. Default: timestamp.')

    parser.add_argument('-tf', '--timeformat', nargs='?', default='%Y-%m-%d %H:%M:%S',
                        help='Timestamp format. Default: \'%%Y-%%m-%%d %%H:%%M:%%S\' e.g.: 1970-01-01 00:00:00')

    parser.add_argument('-tz', '--timezone', default='UTC',
                        help='Timezone of supplied data. Default: UTC')

    parser.add_argument('--fieldcolumns', nargs='?', default='value',
                        help='List of csv columns to use as fields, separated by comma, e.g.: value1,value2. Default: value')

    parser.add_argument('--tagcolumns', nargs='?', default='host',
                        help='List of csv columns to use as tags, separated by comma, e.g.: host,data_center. Default: host')

    parser.add_argument('-x', '--ignorenancolumns', nargs='?', default='',
                        help='List of csv columns use to test if nan, e.g.: host,data_center. Default: host')

    parser.add_argument('-g', '--gzip', action='store_true', default=False,
                        help='Compress before sending to influxdb.')

    parser.add_argument('-b', '--batchsize', type=int, default=5000,
                        help='Batch size. Default: 5000.')

    args = parser.parse_args()
    loadCsv(args.input, args.server, args.user, args.password, args.dbname,
            args.metricname, args.timecolumn, args.timeformat, args.tagcolumns,
            args.fieldcolumns, args.gzip, args.delimiter, args.batchsize, args.create,
            args.timezone, args.ssl, args.ignorenancolumns)
