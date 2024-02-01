import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, FloatType
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("spark://10.10.129.196:7077").appName("hdfs_city").getOrCreate()

start = time.time()

fcdData = spark.read.csv("hdfs://localhost:9000/data/fcd.csv", sep=';', inferSchema=True, header=True)

exp_arg = [("geo_x","float"),
           ("geo_y","float"),
           ("distance","float"),
           ("time_s","float"),
           ("time_e", "float"),
           ("type","string")]

def convert_to(data, type):
    if type=="int":
        return int(data)
    elif type=="float" or type=="float":
        return float(data)
    return data

def get_distance(x1,y1,x2,y2):
    return ((x1-x2)**2 + (y1-y2)**2)**0.5

args = sys.argv[1:]
print(args)
if len(args)< len(exp_arg)-1:
    print("Nedovoljno argumenata!")
    sys.exit()

arg_dict={}
for i in range(len(args)):
    arg_dict[exp_arg[i][0]] = convert_to(args[i], exp_arg[i][1])
if not "type" in arg_dict.keys():
    arg_dict["type"] = None

rez = fcdData.filter((fcdData.timestep_time>=arg_dict["time_s"]) &
                     (fcdData.timestep_time<=arg_dict["time_e"]) &
                     (get_distance(fcdData.vehicle_x,fcdData.vehicle_y, arg_dict["geo_x"],arg_dict["geo_y"])<arg_dict["distance"]))

if arg_dict["type"]!=None:
    rez = rez.filter(fcdData.vehicle_type.contains(arg_dict["type"]))

print(rez.count())
rez.show()

end = time.time()
print(end-start,"s")
