if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_workers>"
    exit 1
fi

ssh localhost &

export PDSH_RCMD_TYPE=ssh &

~/Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/bin/hdfs namenode -format &

wait

~/Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/sbin/stop-all.sh &

wait

~/Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/sbin/start-all.sh &

wait
~/Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/bin/hadoop fs -mkdir /data &

wait

~/Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/bin/hadoop fs -put ~/Documents/Danilo/Faks/VelikiSkupoviPodataka/Proj1/fcd.csv /data &

wait

~/Documents/Danilo/Faks/VelikiSkupoviPodataka/hadoop-3.3.6/bin/hadoop fs -put ~/Documents/Danilo/Faks/VelikiSkupoviPodataka/Proj1/emissions.csv /data &

echo "Data placed" &


# Start the master
gnome-terminal -- bash -c "spark-class org.apache.spark.deploy.master.Master;exec bash" &

wait

sleep 5

# Assign the number of workers to a variable
num_workers=$1
# Spark master address
master_address="spark://192.168.100.5:7077"

# Loop to start N Spark workers
for ((i=1; i<=$num_workers; i++)); do
    echo "Starting Spark Worker $i"
    gnome-terminal -- bash -c "spark-class org.apache.spark.deploy.worker.Worker $master_address; exec bash" &
    sleep 1  # Add a small delay between starting each worker
done

wait

echo "All Spark workers started."

echo "Zadatak 1, 0-1200, single"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad1.py local -74 40 10 0 1200 noprint
    wait
    sleep 1
done

echo "Zadatak 1, 0-2400, single"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad1.py local -74 40 10 0 2400 noprint
    wait
    sleep 1
done

echo "Zadatak 1, 0-3600, single"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad1.py local -74 40 10 0 3600 noprint
    wait
    sleep 1
done

echo "Zadatak 1, 0-1200, cluster"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad1.py $master_address -74 40 10 0 1200 noprint
    wait
    sleep 1
done

echo "Zadatak 1, 0-2400, cluster"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad1.py $master_address -74 40 10 0 2400 noprint
    wait
    sleep 1
done

echo "Zadatak 1, 0-3600, cluster"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad1.py $master_address -74 40 10 0 3600 noprint
    wait
    sleep 1
done


echo "Zadatak 2, 0-1200, single"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad2.py local 0 1200 noprint
    wait
    sleep 1
done

echo "Zadatak 2, 0-2400, single"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad2.py local 0 2400 noprint
    wait
    sleep 1
done

echo "Zadatak 2, 0-3600, single"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad2.py local 0 3600 noprint
    wait
    sleep 1
done

echo "Zadatak 2, 0-1200, cluster"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad2.py $master_address 0 1200 noprint
    wait
    sleep 1
done

echo "Zadatak 2, 0-2400, cluster"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad2.py $master_address 0 2400 noprint
    wait
    sleep 1
done

echo "Zadatak 2, 0-3600, cluster"
for ((i=1;i<=5;i++)); do
    spark-submit --master $master_address ./zad2.py $master_address 0 3600 noprint
    wait
    sleep 1
done