# Weekly Reports 

## Week 9

- Preparing node app for migrating to vocms013/vocms071 (needed ablity to deploy and use without sudo permissions);
  - Deployed node app from scratch and written deployment documentation. It involves:
    - Node and npm installation
    - Npm configuration
    - Express, pm2 installation
    - App installation
    - Firewall restriction issue solving for node app
    - Kerberos authentication issue solving for crontabs and hdfs
    - Crontab launching
    - App launcing
  - Written manage script for managing app with specific commands (sysboot/start/restart/graceful/stop/version/help). For this node is used pm2 process manager, so most of the commands are implemented using pm2 provided methods.
  - Solving pm2 issues (it is not working as non-root user). What is done so far:
    - Installed node and npm within local area that it would be no need to access protected directories.
    - Configured npm to use local area for global installations (such as pm2).
    - Added iptables rules for node and pm2 used ports
    - Problem is still not solved...
- Improvements of webGUI
  - Implemented several charts using static data.
  - Loading multiple csv files
