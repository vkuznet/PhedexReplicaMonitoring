# Weekly Reports 

## Week 11

- Decided to use elasticsearch/kibana for data visualization
- Setup and configure elasticsearch on virtual machine
- Setup and configure kibana on virtual machine
- Decided to export data in elasticsearch directly by running pbr.py script in spark
  - Used new package: elasticsearch-hadoop.jar
  - Added configuration file for script (PBR_CONFIG). File must have section ElasticSearch, elements: node, port and resource (index/type)
  ```
  [ElasticSearch]
  node=128.142.137.188
  port=9200
  resource=block-replicas/sum
  ```
  - Added possibility to write spark job results in elasticsearch in parrallel to hdfs
  ```
   if opts.es:
            validateEsParams(esnode, esport, esresource)
            aggres = aggres.withColumn("origin", lit(opts.esorigin))
            aggres.repartition(1).write.format("org.elasticsearch.spark.sql").option("es.nodes", esnode)\
                                                                             .option("es.port", esport)\
                                                                             .option("es.resource", esresource)\
                                                                             .save(mode="append")
  ```
- Found solution for repetetive data problem. Added new column "origin" which specifies the origin of the data. When using as cronjob it should have value of cronjob. Running script manually user should specify origin by himself (or leave empty - origin "custom"). This field should be later used for making searches in kibana (to select proper data, ex.: origin : cronjob)
- Added new avg-day aggregation for summing bytes and dividing by distinct count of dates
```
ndf.cache()
datescount = ndf.select(ndf.now).distinct().count(
# ....
 aggres = ndf.groupBy(keys).agg(resAgg_dic).orderBy(order, ascending=asc)
# ....
for field in resfields:
  aggres = aggres.withColumn(field, getattr(aggres, field)/datescount)
```
