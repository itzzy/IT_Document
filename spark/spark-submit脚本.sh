###spark-submit脚本
---
###甘意
```
#!/bin/bash
set -e
spark-submit 
--class com.bdp.main.DataEfficiency_Click_log 
--driver-memory 4G 
--executor-cores 4  
--executor-memory 8G  
--num-executors  6    
--master yarn-cluster   
--conf 'spark.kryoserializer.buffer.max=128'  
--files /home/appuser/bdp/sparkjob/erp2-log-task/hive-site.xml,
/usr/hdp/current/hive-client/conf/conf.server/ranger-hive-audit.xml 
--jars /home/appuser/bdp/sparkjob/erp2-log-task/all-dep-bdp-bdp-task.jar,
/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar,
/usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar,
/usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar 
/home/appuser/kettle/data_import/azkaban_sh/dw_log/dw_log_spk_efficiency_click.jar
```

###海涛
```
nohup 
spark-submit  
--master yarn 
--deploy-mode cluster 
--name "app_realtime_analyse" 
--num-executors 10 
--executor-memory 2G  
--executor-cores 2  
realtime-data-calculate-1.3.0-jar-with-dependencies.jar 10 &
```
![yarn-cluster](/Users/zzy/Documents/zzy/code_github/IT_Document/spark/img/yarn-cluster--haitao.jpg)