if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_workers>"
    exit 1
fi

# Start ssh
ssh localhost &

# Find hdfs
cd Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/sbin &

./hdfs namenode -format &

cd ../sbin &

# Start all nodes 
./start-all.sh &

cd ../bin &

# Put data
./hadoop fs -mkdir /data &
./hadoop fs -put ../../Proj1/fcd.csv /data &
./hadoop fs -put ../../Proj1/emissions.csv /data &

echo "Data placed" &

# Start the master
gnome-terminal -- bash -c "ssh localhost && spark-class org.apache.spark.deploy.master.Master; exec bash" &

# Assign the number of workers to a variable
num_workers=$1
# Spark master address
master_address="spark://localhost:7077"

# Loop to start N Spark workers
for ((i=1; i<=$num_workers; i++)); do
    echo "Starting Spark Worker $i"
    gnome-terminal -- bash -c "ssh localhost && spark-class org.apache.spark.deploy.worker.Worker spark://$master_address; exec bash" &
    sleep 1  # Add a small delay between starting each worker
done

echo "All Spark workers started."

spark

echo "Prvi zadatak, local, 0-1200"
for ((i=1;i<=5;i++)); do
    spark-submit --master spark://localhost:7077 ./zad1.py local -74 40 10 0 1200 noprint &
done

# echo "Prvi zadatak, local, 0-2400"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark &&  spark-submit --master spark://localhost:7077 ./zad1.py local -74 40 10 0 2400 noprint" &
# done

# echo "Prvi zadatak, local, 0-3600"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark &&  spark-submit --master spark://localhost:7077 ./zad1.py local -74 40 10 0 3600 noprint" &
# done

# echo "Prvi zadatak, cluster, 0-1200"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark &&  spark-submit --master spark://localhost:7077 ./zad1.py spark://localhost:7077 -74 40 10 0 1200 noprint" &
# done

# echo "Prvi zadatak, cluster, 0-2400"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark &&  spark-submit --master spark://localhost:7077 ./zad1.py spark://localhost:7077 -74 40 10 0 2400 noprint" &
# done

# echo "Prvi zadatak, cluster, 0-3600"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark &&  spark-submit --master spark://localhost:7077 ./zad1.py spark://localhost:7077 -74 40 10 0 3600 noprint" &
# done


# echo "Drugi zadatak, local, 0-1200"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark && spark-submit --master spark://localhost:7077 ./zad2.py local 0 1200 noprint" &
# done

# echo "Drugi zadatak, local, 0-2400"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark && spark-submit --master spark://localhost:7077 ./zad2.py local 0 2400 noprint" &
# done

# echo "Drugi zadatak, local, 0-3600"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark && spark-submit --master spark://localhost:7077 ./zad2.py local 0 3600 noprint" &
# done

# echo "Drugi zadatak, cluster, 0-1200"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark && spark-submit --master spark://localhost:7077 ./zad2.py spark://localhost:7077 0 1200 noprint" &
# done

# echo "Drugi zadatak, cluster, 0-2400"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark && spark-submit --master spark://localhost:7077 ./zad2.py spark://localhost:7077 0 2400 noprint" &
# done

# echo "Drugi zadatak, cluster, 0-3600"
# for ((i=1;i<=5;i++)); do
#     gnome-terminal -- bash -c "ssh localhost && spark && spark-submit --master spark://localhost:7077 ./zad2.py spark://localhost:7077 0 3600 noprint" &
# done

# echo "Done"