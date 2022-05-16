import math
import csv
from statistics import median
import json
import pandas as pd
from pyspark.sql import SparkSession
import pyspark
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

import sys
from io import StringIO

import shapely
from shapely.geometry import Point
from pyproj import Transformer

def get_nyc_cbg(csv_file):
	csv_reader = csv.reader(open(csv_file))
	next(csv_reader)
	cbg = {}
	for row in csv_reader:
		cbg[row[0]] = t.transform(float(row[1]), float(row[2]))
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
      dis = calculate(visitor_home_cbgs, poi_cbg)
      if date_range_start in valid_range:
        return poi_cbg, (date_range_start, dis)
      else:
        return poi_cbg, (date_range_end, dis)

def distance(poi_cbg, home_cbg):
  distance = Point(nyc_cbg[poi_cbg][0], nyc_cbg[poi_cbg][1]).distance(Point(nyc_cbg[home_cbg][0], nyc_cbg[home_cbg][1]))/5280

  return distance

def calculate(a, b):
  visitors = json.loads(a)
  dis = []
  for home_cbg in visitors:
    if home_cbg in nyc_cbg:
      tmp = distance(b, home_cbg)
      for i in range(int(visitors[home_cbg])):
        dis.append(tmp)
  return dis

def med_dis(l):
	if l == []:
		return None
	return str(round(median(l), 2))

def group_mapper(tp):
  key = tp[0]
  dis_dict = {"2019-03": [], "2019-10": [], "2020-03": [], "2020-10": []}
  for val in tp[1]:
    date_range = val[1][0]
    dis = val[1][1]
    dis_dict[date_range].extend(dis)
      
  return key, med_dis(dis_dict["2019-03"]), med_dis(dis_dict["2019-10"]), med_dis(dis_dict["2020-03"]), med_dis(dis_dict["2020-10"])

if __name__ == "__main__":
    t = Transformer.from_crs(4326, 2263)
    
    nyc_place_key = set(map(lambda x: x[9], pd.read_csv('nyc_supermarkets.csv').to_numpy()))
    nyc_cbg = get_nyc_cbg("nyc_cbg_centroids.csv")

    weekly = sc.textFile("/tmp/bdm/weekly-patterns-nyc-2019-2020/", use_unicode=True)
    rdd = weekly.map(filter_mapper).filter(lambda x: x is not None).groupBy(lambda x: x[0]).map(group_mapper)
    rdd.cache()
    
    df = spark.createDataFrame(rdd)
    output = df.toPandas()
    output.columns = ['cbg_fips', '2019-03', '2019-10', '2020-03', '2020-10']
    output = output.fillna('').sort_values('cbg_fips')
    output = spark.createDataFrame(output)
    output.write.options(header='true').csv(sys.argv[1])