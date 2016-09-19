# Weekly Reports 

## Week 8

- Pbr.py script produces many logs in execution mode. Added different log level support for this issue.
```
# In the script execution parameter might be added
# Possible parameter values: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
--logs INFO
```
- Added option pbr.py to collect results into one node and save it as json (--collect).
```
aggres = aggres.toJSON().collect()
with open(fout_header, 'w+') as f:
    f.write('[')
    f.write(",".join(aggres))
    f.write(']')
```
- Tested different approaches to get data from hdfs to local file system:
  - WebHdfs npm package. Ability to get data from hdfs using http requests. Problem: not working with kerberos authentication.
  - Eclairjs npm package. Ability to get spark instanace running during app lifecycle. Possibility to collect results (and even do more distributed calculations) to one node and use for visualization. Problem: requires a lot of dependencies (JAVA JRE 1.8, JAVA JDK 1.8, JAVA DEVEL 1.8, MAVEN, JUPYTER, APACHE TOREE) - would be difficult to deploy and maintain on different cluster.
  - WebHdfs commands. Ability to get data from hdfs using http requests. Problem: limited usage, can get only one partition - need for implementing and maintaining merge of partitions, etc.
  - Hdfs file system commands. Ability to write hdfs fs commands and get result to local file system (drawback: need to maintain and clean local area). Best solution:
    - Used getmerge command in order to get all partitions at once.
    - Developed crontab jobs for collecting data to local area. In addition in writes header in collected file in order to get read it properly with d3 in csv method.
    ```
    # Configurable parameters: source directory in hdfs and output directory in fs
    now=$(date +'%Y-%m-%d')
    filename=`date +%Y-%m-%d -d "yesterday"`
    headers=now,br_user_group,data_tier,acquisition_era,node_kind,br_dest_bytes,br_node_bytes
    
    hadoop fs -getmerge $1/$now* $2/$filename
    sed -i '1s/^/'$headers'\n/' $2/$filename
    ```
    - Developed cron job for cleaning data from local area.
    ```
    # Configurable parameters: source directory in fs and data preservation period in months
    filename=`date +%Y-%m-%d -d "${2} months ago"`
    
    if [ -f $1/$filename ]; then
        rm $1/$filename
    fi
    ```
    - Solved kerberos ticket issue.
      - Creating keytab file.
      - Changing its permissions:
      ```
      chmod u+rw,g-rwx,g-rwx username.keytab
      ```
      - Adding lines in crontab scripts for reading your credentials and invoking kinit for you without asking for your password.
      ```
      export KEYTAB=/your_path/username.keytab
      principal=`klist -k $KEYTAB | tail -1 | awk '{print $2}'`
      kinit $principal -k -t $KEYTAB
      ```
- Setup pbr.py service on vocms013/vocms071 nodes and executed crontab job.
  - It performs daily aggregation and saves results in hdfs /cms/phedex-monitoring
  - Chosen parameters:
  ```
  local fromdate=`date +%Y-%m-%d -d "yesterday"` # GNU date utility
  local todate=$fromdate # make daily snapshots
  local keys=now,br_user_group,data_tier,acquisition_era,node_kind
  local results=br_dest_bytes,br_node_bytes
  case $action in
    prmonitor )
      $PHEDEXREPLICAMONITORING_ROOT/bin/pbr.sh --yarn \
         --basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
         --fromdate $fromdate \
        --todate $todate \
        --keys $keys \
         --results $results \
         --aggregations sum \
         --order br_node_bytes \
         --asc 0 \
         --fout $HDFS_MIGRATE \
         --verbose 2>&1 1>& $LOGDIR/prmonitor.log
       ;;
     * )
  ```
