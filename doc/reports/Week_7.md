# Weekly Reports 

## Week 7

- Renamed project from Phedex-ReplicaMonitoring to PhedexReplicaMonitoring due to maintanability reasons. Dash in the name of project is difficult to maintain (in python cannot import package with dash in its name).
- Reading and understanding concepts of:
  - Node.js
  - Npm
  - Express
  - Jade
  - Pm2
- Set up virtual machine for webGUI development and testing (cms-phedex-plots).
- Solved firewall issues for running node app on specific port:
```
iptables -I INPUT 66 -p tcp -d 128.142.137.188 --dport 8880 -j ACCEPT
# Replace ip address(128.142.137.188) and port(8880) with specific on machines running (port should be also changed in application_dir/process.yml)
# Replace line number(66) with specific one (ex.: last INPUT rule was denying connections)
# Depending on iptables on specif machine it might be necessary to add an output rule
```
- Faced permission issues. PM2 can be run only with the sudo rights. Needed further investigation.
- Ran basic node app for testing reasons.
- Produced plan for WebGUI implementation
  - Pbr.py job runs daily and collects results on hdfs /cms/phedex-monitoring.
  - Collect.sh and remove.sh runs daily to collect data from hdfs to local file system and clean local file system (specific directory will be specified later).
  - Node app reads data from local file system.
  - Node app displays:
    - Current data. Todays block replica node and destination bytes grouped by node kind/user group/data tier /acquisition era. 7 days summary of previously mentioned data types.
    - Filter data. User have a possibility to choose node kind/user group/acquisition era/date interval. Depending on choice user gets visualized data.
