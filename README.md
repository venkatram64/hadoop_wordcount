1. su - hduser

2. cd /usr/local/hadoop

3. bin/hdfs dfs -mkdir /user

4. bin/hdfs dfs -mkdir /user/venkat

5. create a sample file /home/venkatram/Desktop/data as sample.txt

6. bin/hdfs dfs -put /home/venkatram/Desktop/data /user/input

7. bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.wordcount.WordCount /user/input output2

9. bin/hdfs dfs -cat output2/*

10. http://localhost:50070


******************************
to install git on ubuntu

sudo apt-get install git-core