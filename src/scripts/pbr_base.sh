#!/bin/sh
# Author: Aurimas Repecka <aurimas.repecka AT gmail [DOT] com>
# A wrapper script to submit spark job with pbr.sh script

bash pbr.sh --yarn \
            --basedir hdfs:///project/awg/cms/phedex/block-replicas-snapshots/csv/ \
            --fromdate 2016-09-10 \
            --todate 2016-09-15 \
            --aggregations sum \
            --results br_dest_bytes,br_node_bytes \
            --interval 1 \
            --fout hdfs:///user/arepecka/ReplicaMonitoring \
            --keys now,br_user_group,data_tier,acquisition_era,node_kind \
            --order br_node_bytes \
            --asc 0 \
            --es
            #--header
            #--collect
            #--logs error
            #--filt node_name:T2_US_Florida \
            #--order now,node_name \
            #--asc 0,1 \
            #--verbose \
            #--fname /home/aurimas/CERN/ReplicaMonitoring/v2/data/project/awg/cms/phedex/block-replicas-snapshots/csv/time=2016-07-09_03h07m28s
            #--results br_dest_bytes,br_node_bytes \
            #--interval 1 \
            #--fout hdfs:///user/arepecka/ReplicaMonitoring \
            #--keys now,br_user_group,data_tier,acquisition_era,node_kind \
            #--order br_node_bytes \
            #--asc 0 \
            #--es


