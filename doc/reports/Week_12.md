# Weekly Reports 

## Week 12

- Developing script for transfering data from hdfs to elasticsearch manually (using spark)
  - Ability to run script both: locally and using yarn manager (parameter --yarn)
  - Reading one or multiple HDFS files with any number of partitions. Parameters: --fname (for one file), --basedir (for many files) can be specified. Files can be filtered using dates parameters (--fromdate, --todate). 
  - Configurable elasticsearch parameters: node, port, resource(index/type). Everything must be specified inside hdfs-es.cfg file with section [Elasticsearch].
  - CSV files support
  - Data schema applied dynamically from user specified json file. User specified file must contain attributes of name, type and nullable. Example can be found in data/schema.json
  - Ability to specify new column "origin" values for data distinction from other executions
- Creating dashboards for actual uses (later these objects will be exported to vocms013 kibana configuration)
  - Creating pie charts for top 5 user groups splited by top 20 data tiers and acquisition eras. Result value: node bytes.
  - Creating bar charts for top 3 user groups splited by top 20 data tiers and acquisition eras in perspective of time dimension. Result value: node bytes.
- Added new grouping keys:
  - Node tier: two first symbols of node name.
  - Campaign: the segment between dashes before processing version. Ex.: X in /A/B-C-...-V-X-Y/Z.
- Disabled scientific format for numbers in csv files.
- Making presentation for meeting
