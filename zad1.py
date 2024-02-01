from pyspark.sql import SparkSession
import time
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def get_args():
    exp_arg = [("master","string"),
           ("geo_x","float"),
           ("geo_y","float"),
           ("distance","float"),
           ("time_s","float"),
           ("time_e", "float"),
           ("type","string"),
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

def get_distance(x1,y1,x2,y2):
    return ((x1-x2)**2 + (y1-y2)**2)**0.5

def print_output(rez):
    print(rez.count())
    rez.show()

arg_dict = get_args()

spark = SparkSession.builder.master(arg_dict["master"]).appName("hdfs_city").getOrCreate()
start = time.time()
fcdData = spark.read.csv("hdfs://localhost:9000/data/fcd.csv", sep=';', inferSchema=True, header=True)

rez = fcdData.filter((fcdData.timestep_time>=arg_dict["time_s"]) &
                     (fcdData.timestep_time<=arg_dict["time_e"]) &
                     (get_distance(fcdData.vehicle_x,fcdData.vehicle_y, arg_dict["geo_x"],arg_dict["geo_y"])<arg_dict["distance"]))

if arg_dict["type"]!=None:
    rez = rez.filter(fcdData.vehicle_type.contains(arg_dict["type"]))
end = time.time()


with open("logs.txt",'a') as f:
    time_s = arg_dict["time_s"]
    time_e = arg_dict["time_e"]
    tip = "Samo master" if arg_dict["master"] == "local" else "Cluster"
    params = "X: "+str(arg_dict["geo_x"])+\
             " Y: "+str(arg_dict["geo_y"])+\
             " D: "+str(arg_dict["distance"])
    if arg_dict["type"] !=None:
        params +=" Type: "+arg_dict["type"]

    print("Zadatak 1, "+tip + " "+params+" "+str(time_s)+"-"+str(time_e),file=f)
    
if arg_dict["print"]:
    print_output(rez)