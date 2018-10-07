1. su - hduser

# if haddop is not running, run the following commands go to  cd /usr/local/hadoop/sbin

      1. start-dfs.sh

      2. start-yarn.sh

      3. jps(this command will show all the running processes)


2. cd /usr/local/hadoop

3. bin/hdfs dfs -ls /user

#The followings are required only first time you are running jobs.

    1. bin/hdfs dfs -mkdir /user

    2. bin/hdfs dfs -mkdir /user/venkat

# copy the test files into hadoop dir

5. create a sample file /home/venkatram/Desktop/data as sample.txt

6. bin/hdfs dfs -put /home/venkatram/Desktop/data /user/input

7. bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.wordcount.WordCount /user/input output2

9. bin/hdfs dfs -cat output2/*

10. http://localhost:50070


******************************
to install git on ubuntu

sudo apt-get install git-core