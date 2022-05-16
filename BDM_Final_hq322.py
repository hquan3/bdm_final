import math
import csv
import json
from pyspark.sql import SparkSession
import pyspark

import sys
from io import StringIO


def get_nyc_supermarkets(csv_file):
	csv_reader = csv.reader(open(csv_file))
	next(csv_reader)
	place_keys = []
	for row in csv_reader:
		place_keys.append(row[9])
	return set(place_keys)


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
		if place_key in nyc_place_key and poi_cbg in nyc_cbg and (
				date_range_end in valid_range or date_range_start in valid_range):
			json_obj = json.loads(visitor_home_cbgs)
			total_dis = 0
			cnt = 0
			for home_cbg in json_obj:
				if home_cbg in json_obj:
					tmp = get_distance(poi_cbg, home_cbg)
					if tmp > 0:
						total_dis += tmp
						cnt += 1

			if date_range_start in valid_range:
				return poi_cbg, (date_range_start, total_dis, cnt)
			else:
				return poi_cbg, (date_range_end, total_dis, cnt)


def group_mapper(tp):
	key = tp[0]

	dis_dict = {"2019-03": [0, 0], "2019-10": [0, 0], "2020-03": [0, 0], "2020-10": [0, 0]}
	for val in tp[1]:
		date_range = val[1][0]
		dis = val[1][1]
		cnt = val[1][2]
		dis_dict[date_range][0] += dis
		dis_dict[date_range][1] += cnt

	return key, cal_avg_dis(dis_dict["2019-03"]), cal_avg_dis(dis_dict["2019-10"]), cal_avg_dis(
		dis_dict["2020-03"]), cal_avg_dis(dis_dict["2020-10"])


def cal_avg_dis(l: list):
	if l[1] == 0:
		return 0

	return l[0] / l[1]


def get_distance(poi_cbg, home_cbg):
	if home_cbg not in nyc_cbg:
		return 0

	tmp1 = pow(nyc_cbg[poi_cbg][0] - nyc_cbg[home_cbg][0], 2)
	tmp2 = pow(nyc_cbg[poi_cbg][1] - nyc_cbg[home_cbg][1], 2)
	return math.sqrt(tmp1 + tmp2)


if __name__ == '__main__':
	nyc_place_key = get_nyc_supermarkets("nyc_supermarkets.csv")
	nyc_cbg = get_nyc_cbg("nyc_cbg_centroids.csv")

	sc = pyspark.SparkContext.getOrCreate()
	sc.broadcast(nyc_place_key)
	sc.broadcast(nyc_cbg)

	output = sys.argv[1]
	res_rdd = sc.textFile("/tmp/bdm/weekly-patterns-nyc-2019-2020/") \
		.repartition(20)\
		.map(filter_mapper) \
		.filter(lambda x: x is not None) \
		.groupBy(lambda x: x[0], numPartitions=20) \
		.map(group_mapper)

	spark = SparkSession(sc)
	df = spark.createDataFrame(res_rdd, ["cbg_fips", "2019-03", "2019-10", "2020-03", "2020-10"])
	df.write.options(header='true').csv(output)
