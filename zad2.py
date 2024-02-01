import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, FloatType
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.master("local").appName("hdfs_city").getOrCreate()

start = time.time()

emsData = spark.read.csv("hdfs://localhost:9000/data/emissions.csv", sep=';', inferSchema=True, header=True)

exp_arg = [("time_s","float"),
           ("time_e", "float")]

def convert_to(data, type):
    if type=="int":
        return int(data)
    elif type=="float" or type=="float":
        return float(data)
    return data

def get_processing(name):
    n = "vehicle_"+name
    return [F.min(n).alias("min_"+name),
            F.max(n).alias("max_"+name),
            F.mean(n).alias("mean_"+name),
            F.stddev(n).alias("stddev_"+name)]

def get_all_processing(names):
    rez = []
    for name in names:
        for x in get_processing(name):
            rez.append(x)
    return rez

args = sys.argv[1:]
if len(args)< len(exp_arg)-1:
    print("Nedovoljno argumenata!")
    sys.exit()

arg_dict={}
for i in range(len(args)):
    arg_dict[exp_arg[i][0]] = convert_to(args[i], exp_arg[i][1])
if not "type" in arg_dict.keys():
    arg_dict["type"] = None

q = "timestep_time"
rez = emsData.filter((emsData.timestep_time>=arg_dict["time_s"]) &
                     (emsData.timestep_time<=arg_dict["time_e"]))\
                    .groupBy("vehicle_lane").agg(
                        *get_all_processing(["CO2","CO","HC","NOx","PMx","noise","fuel","electricity"])
                    )

print(rez.count())
rez.show()

end = time.time()
print(end-start,"s")
