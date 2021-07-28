#!/bin/bash

curl -X POST http:// --header "Content-Type:appication/json;charset=UTF-8" --data '{
    "appResource": "..../PySparkSubmit.py"
    "sparkProperties": {
        "spark.executor.memory": "512m",
        "spark.master": "spark://nodo1:7077",
        "spark.driver.memory": "512m",
        "spark.driver.cores": "2",
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": "file:/....",
        "spark.history.fs.logDirectory": "file:"/....",
        "spark.app.name": "PySpark REST API",
        "spark.submit.deployMode": "cluster",
        "spark.driver.supervise": "true"
    },
    "clientSparkVersion": "3.1.1",
    "mainClass": "PySparkSubmit",
    "environmentVariables": {
        "SPARK_ENV_LOADED": "1"
    },
    "action": "CreateSubmissionRequest",
    "appArgs": [
        "..../PySparkSubmit.py",
        "alluxio://alluxio-master:19998/mnt/hdfs/carlos"
    ]
}'