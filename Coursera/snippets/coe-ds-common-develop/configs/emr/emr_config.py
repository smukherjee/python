"""
EMR Configuration
"""

emr_config = [
  {
    "Classification": "yarn-site",
    "Properties": {
      "yarn.nodemanager.pmem-check-enabled": "false",
      "yarn.nodemanager.vmem-check-enabled": "false",
      "yarn.nodemanager.resource.memory-mb": "64512",
      "yarn.nodemanager.resource.cpu-vcores": "15"
    },
    "Configurations": []
  },
  {
    "Classification": "spark",
    "Properties": {
      "maximizeResourceAllocation": "false"
    },
    "Configurations": []
  },
  {
    "Classification": "spark-defaults",
    "Properties": {
      # "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
      # "spark.jars.packages": "io.delta:delta-core_2.11:0.4.0",
      "spark.executor.memory": "21000M",
      "spark.driver.memory": "21000M",
      "spark.yarn.scheduler.reporterThread.maxFailures": "5",
      "spark.scheduler.mode": "FAIR",
      "spark.driver.memoryOverhead": "21000M",
      "spark.executor.heartbeatInterval": "60s",
      "spark.rdd.compress": "true",
      "spark.network.timeout": "8000s",
      "spark.driver.cores": "5",
      "spark.executor.cores": "5",
      "spark.memory.storageFraction": "0.30",
      "spark.shuffle.spill.compress": "true",
      "spark.shuffle.compress": "true",
      "spark.storage.level": "MEMORY_AND_DISK_SER",
      "spark.default.parallelism": "1700",
      "spark.memory.fraction": "0.80",
      "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError=\"kill -9 %p\"",
      "spark.executor.instances": "170",
      "spark.executor.memoryOverhead": "21000M",
      "spark.dynamicAllocation.enabled": "false",
      "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError=\"kill -9 %p\""
    },
    "Configurations": []
  },
  {
    "Classification": "mapred-site",
    "Properties": {
      "mapreduce.map.output.compress": "true"
    },
    "Configurations": []
  },
  {
    "Classification": "hadoop-env",
    "Properties": {},
    "Configurations": [
      {
        "Classification": "export",
        "Properties": {
          "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
        },
        "Configurations": []
      }
    ]
  },
  {
    "Classification": "spark-env",
    "Properties": {},
    "Configurations": [
      {
        "Classification": "export",
        "Properties": {
          "PYSPARK_PYTHON": "/usr/bin/python3",
          "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
        },
        "Configurations": []
      }
    ]
  },
  {
      "Classification": "jupyter-s3-conf",
      "Properties": {
          "s3.persistence.enabled": "true",
          "s3.persistence.bucket": "MyJupyterBackups"
      }
  }
]