#!/bin/bash

echo "=============================================================="
echo "=================== Script Runner 1.0 ========================"
echo "=================== Big Data Project (CS522) ================="
echo "=================== By: Jivan Nepali, 985095 ================="
echo "=============================================================="

#Initialize Settings
ALGO_TYPE="spark"
HDFS_DIR="/usr/jivan"
LOCAL_DIR="/home/cloudera/project"

/bin/cp -f /home/cloudera/workspace/sparkproject/target/*.jar /home/cloudera/project/spark


echo "________1. creating the directory for $ALGO_TYPE in HDFS_______"
hadoop fs -test -d "$HDFS_DIR/$ALGO_TYPE" && echo "$ALGO_TYPE directory already exists" || hadoop fs -mkdir $HDFS_DIR/$ALGO_TYPE
hadoop fs -test -d "$HDFS_DIR/$ALGO_TYPE/input" && echo "$ALGO_TYPE/input directory already exists" || hadoop fs -mkdir $HDFS_DIR/$ALGO_TYPE/input


echo "________2. copying input files from local FS to HDFS_________"
echo "first, deleting the existing i/p files"
for filename in $(hadoop fs -ls $HDFS_DIR/$ALGO_TYPE/input/*.log); do
	if [[ $filename == *.log ]]
	then
		echo "deleting $filename"
		hadoop fs -rm "$filename"
	fi
done

#echo "displaying input doc ..."
#for filename in $LOCAL_DIR/$ALGO_TYPE/input/*.log; do
#	cat $filename
#done

echo "copying the files ..."
hadoop fs -copyFromLocal $LOCAL_DIR/$ALGO_TYPE/input/*.log $HDFS_DIR/$ALGO_TYPE/input

#echo "______3. deleting the output directory for $ALGO_TYPE in HDFS_______"
#if $(hadoop fs -test -d "$HDFS_DIR/$ALGO_TYPE/output")
#then
#	for filename in $(hadoop fs -ls $HDFS_DIR/$ALGO_TYPE/output/*); do
#		if ( [[ $filename == *_SUCCESS ]] || [[ $filename == *part-r-* ]] );then
#			echo "deleting $filename"
#			hadoop fs -rm "$filename"
#		fi
#	done
#	hadoop fs -rm -r $HDFS_DIR/$ALGO_TYPE/output
#else
#	echo "the output directory does not exist, nothing to delete!"
#fi

echo "_____________4. executing the jar in for $ALGO_TYPE ___________"
echo "changing file permission for jar file"
chmod 777 $LOCAL_DIR/$ALGO_TYPE/sparkproject-0.0.1-SNAPSHOT.jar
spark-submit --class cs522.sparkproject.LogAnalyzer $LOCAL_DIR/$ALGO_TYPE/sparkproject-0.0.1-SNAPSHOT.jar  $HDFS_DIR/$ALGO_TYPE/input $HDFS_DIR/$ALGO_TYPE/output
echo "the job execution has completed"


echo "___________5. copying the output files from HDFS to local FS ______________"
test -d "$LOCAL_DIR/$ALGO_TYPE/output" && echo "the output directory in local FS already exists" || mkdir $LOCAL_DIR/$ALGO_TYPE/output
for filename in $LOCAL_DIR/$ALGO_TYPE/output/*/*; do
	echo "deleting $filename"
	rm -f $filename
done
echo "file copy started ..."
for filename in $(hadoop fs -ls $HDFS_DIR/$ALGO_TYPE/output/*/*);do
	if ( [[ $filename == *_SUCCESS ]] || [[ $filename == *part-* ]] );then
		outputType=$(basename $(dirname $filename))
		test -d "$LOCAL_DIR/$ALGO_TYPE/output/$outputType" && echo "the output directory $outputType in local FS already exists" || mkdir $LOCAL_DIR/$ALGO_TYPE/output/$outputType
		echo "copying file $filename ..."
		hadoop fs -copyToLocal $filename $LOCAL_DIR/$ALGO_TYPE/output/$outputType
	fi
done	



echo "___________6. displaying the output result _____________"
for filename in $LOCAL_DIR/$ALGO_TYPE/output/*/*; do
	if ( [[ $filename == *part* ]] ); then
		echo "displaying content inside $filename"
		echo "___________________________________"
		cat $filename
	fi
done


echo "============================================"
echo "=====Script Runner 1.0 Completed!!=========="
echo "============================================"

