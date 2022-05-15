import math
from math import sin, cos, sqrt, atan2, radians

import csv
import json
import pandas as pd
from pyspark.sql import SparkSession
import pyspark

import sys
from io import StringIO

sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

def get_nyc_cbg(csv_file):
	csv_reader = csv.reader(open(csv_file))
	next(csv_reader)
	cbg = {}
	for row in csv_reader:
		cbg[row[0]] = (float(row[1]), float(row[2]))
	return cbg

def filter_mapper(record):
    reader = csv.reader(StringIO(record), delimiter=",")
    valid_range = ["2019-03", "2019-10", "2020-03", "2020-10"]
    for row in reader:
        place_key = row[0]
        poi_cbg = row[18]
        visitor_home_cbgs = row[19]
        date_range_end = row[12][0: 7]
        date_range_start = row[13][0: 7]
        if place_key in nyc_place_key and poi_cbg in nyc_cbg and (date_range_end in valid_range or date_range_start in valid_range):
            total_dis, count = calculate(visitor_home_cbgs, poi_cbg)
            if date_range_start in valid_range:
                return poi_cbg, (date_range_start, total_dis, count)
            else:
                return poi_cbg, (date_range_end, total_dis, count)

def distance(poi_cbg, home_cbg):
    R = 6373.0

    lat1 = radians(nyc_cbg[poi_cbg][0])
    lon1 = radians(nyc_cbg[poi_cbg][1])
    lat2 = radians(nyc_cbg[home_cbg][0])
    lon2 = radians(nyc_cbg[home_cbg][1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c * 0.62137
    return distance

def calculate(a, b):
    visitors = json.loads(a)
    total_dis = 0
    count = 0
    for home_cbg in visitors:
        if home_cbg in nyc_cbg:
            tmp = distance(b, home_cbg)
            if tmp > 0:
                total_dis += tmp * visitors[home_cbg]
                count += visitors[home_cbg]
    return total_dis, count

def avg_dis(l):
	if l[1] == 0:
		return None
	return str(round(l[0] / l[1], 2))

def group_mapper(tp):
	key = tp[0]

	dis_dict = {"2019-03": [0, 0], "2019-10": [0, 0], "2020-03": [0, 0], "2020-10": [0, 0]}
	for val in tp[1]:
		date_range = val[1][0]
		dis = val[1][1]
		cnt = val[1][2]
		dis_dict[date_range][0] += dis
		dis_dict[date_range][1] += cnt

	return key, avg_dis(dis_dict["2019-03"]), avg_dis(dis_dict["2019-10"]), avg_dis(dis_dict["2020-03"]), avg_dis(dis_dict["2020-10"])

if __name__ == "__main__":
    nyc_place_key = set(map(lambda x: x[9], pd.read_csv('nyc_supermarkets.csv').to_numpy()))
    nyc_cbg = get_nyc_cbg("nyc_cbg_centroids.csv")

    weekly = sc.textFile("/tmp/bdm/weekly-patterns-nyc-2019-2020/", use_unicode=True)
    rdd = weekly.map(filter_mapper).filter(lambda x: x is not None).groupBy(lambda x: x[0]).map(group_mapper)

    df = spark.createDataFrame(rdd)
    output = df.toPandas()
    output.columns = ['cbg_fips', '2019-03', '2019-10', '2020-03', '2020-10']
    output = output.fillna('').sort_values('cbg_fips')
    output = spark.createDataFrame(output)
    output.write.options(header='true').csv(sys.argv[1])