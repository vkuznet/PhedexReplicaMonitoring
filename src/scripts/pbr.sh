#!/bin/sh
# Author: Aurimas Repecka <aurimas.repecka AT gmail [DOT] com>
# Based On Work By: Valentin Kuznetsov <vkuznet AT gmail [DOT] com>
# A wrapper script to submit spark job with pbr.py script

# test arguments
if [ "$#" -eq 0 ]; then
    echo "Usage: pbr <options>"
    echo "       pbr --help"
    exit 1
fi

# get required jars
if [ ! -f $SPARK_CSV_ASSEMBLY_JAR ]; then
   echo "Unable to find spark-csv-assembly jar in SPARK_CSV_ASSEMBLY_JAR environment"
   exit 1
fi
if [ ! -f $ES_HADOOP_JAR ]; then
   echo "Unable to find elasticsearch-hadoop jar in ES_HADOOP_JAR environment"
   exit 1
fi
jars=$SPARK_CSV_ASSEMBLY_JAR,$ES_HADOOP_JAR

# find our where package is installed on a system
wroot=`python -c "import ReplicaMonitoring; print '/'.join(ReplicaMonitoring.__file__.split('/')[:-1])"`

if [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "-help" ]; then
    # run help
    python $wroot/pbr.py --help
elif [[  $1 =~ -?-yarn(-cluster)?$ ]]; then
    # to tune up these numbers:
    #  - executor-memory not more than 5G
    #  - num-executor can be increased (suggested not more than 10)
    #  - cores = 2/4/8
    # Temp solution to have a wrapper for python27 on spark cluster
    # once CERN IT will resolve python version we can remove PYSPARK_PYTHON
#    PYSPARK_PYTHON='/etc/spark/python' \
    PYSPARK_PYTHON='/afs/cern.ch/user/v/valya/public/python27' \
    spark-submit \
        --master yarn-client \
        --driver-class-path '/usr/lib/hive/lib/*' \
        --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' \
        --executor-memory 5g \
        --jars $jars \
        $wroot/pbr.py ${1+"$@"}
else
    PYSPARK_PYTHON='/afs/cern.ch/user/v/valya/public/python27' \
    spark-submit \
        --driver-class-path '/usr/lib/hive/lib/*' \
        --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' \
        --jars $jars \
        --executor-memory $((`nproc`/4))G \
        --master local[$((`nproc`/4))] \
        $wroot/pbr.py ${1+"$@"}
fi


