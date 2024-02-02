from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def get_args():
    exp_arg = [ ("master","string"),
                ("time_s","float"),
                ("time_e", "float"),
                ("print","bool")]
    
    def convert_to(data, type):
        if type=="int":
            return int(data)
        elif type=="float" or type=="float":
            return float(data)
        elif type=="bool":
            return data=="print"
        return data

    args = sys.argv[1:]
    print(args)
    if len(args)< len(exp_arg)-1:
        print("Nedovoljno argumenata!")
        sys.exit(-1)

    arg_dict={}
    for i in range(len(args)):
        arg_dict[exp_arg[i][0]] = convert_to(args[i], exp_arg[i][1])
    if not "type" in arg_dict.keys():
        arg_dict["type"] = None
    if not "print" in arg_dict.keys():
        arg_dict["print"] = True
    
    return arg_dict

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

def print_output(rez):
    print(rez.count())
    rez.show()

arg_dict= get_args()

spark = SparkSession.builder.master(arg_dict["master"]).appName("hdfs_city").getOrCreate()
start = time.time()
emsData = spark.read.csv("hdfs://localhost:9000/data/emissions.csv", sep=';', inferSchema=True, header=True)

rez = emsData.filter((emsData.timestep_time>=arg_dict["time_s"]) &
                     (emsData.timestep_time<=arg_dict["time_e"]))\
                    .groupBy("vehicle_lane").agg(
                        *get_all_processing(["CO2","CO","HC","NOx","PMx","noise","fuel","electricity"])
                    )
rez.show()
end=time.time()

with open("logs.txt",'a') as f:
    time_s = arg_dict["time_s"]
    time_e = arg_dict["time_e"]
    tip = "Samo master" if arg_dict["master"] == "local" else "Cluster"
    print("Zadatak 2, "+tip + " "+str(time_s)+"-"+str(time_e),file=f)
    print("\t Vreme: "+str(end-start)+" s", file=f)

if arg_dict["print"]:
    print_output(rez)