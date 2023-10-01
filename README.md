# Timescale usefull scripts
This repository consists of 2 sub-parts
Repository scructure:
* SQL - analytics scripts, that will help you get along with your TSDB better
* Python - some .py code, that will reduce your time doing some routine job as well as perform seamless chunk migration between your clusters, as well as to VictoriaMetrics

## Prerequisites
Documentations:
- [TimescaleDB](https://www.timescale.com/)
- [Timescale chunks & hypertables](https://docs.timescale.com/api/latest/hypertable/)
- [VictoriaMetrics](https://victoriametrics.com/)
- [VictoriaMetricsCluster] (https://docs.victoriametrics.com/Cluster-VictoriaMetrics.html)

## SQL
* analytics_access_node.sql - TimescaleDB singlenode and multinode analytics
* chunks_movement_analysis.sql - Some basic scripts, that will help to analyze TimescaleDB chunks size, conditions and need for chunks mevement. Also some automation scripts, that can ease your routine

## Python
* ChunkCompression - sometimes, when your TSDB cluster fails, you might face an abnormal behaviour, when your chunk compression policies on AN differ from DN. This code will help you align and normalize this problem
* ChunkDataClusterMigration - Unfortunately, Timescaledb does not support recovery, after your whole Access Node crashes, and no metadata stored on it can be recovered. In other words, you can not connect your filled DataNodes with fresh empty AccessNode without any data - manipulations. So, this code is one of the solutions. You can set up a whole new empty Timescale cluster, prefill it with needed hypertables to start data ingestion (or do it automatically, using promscale or other metrics management solutions), and migrate data from your old broken cluster
* VictoriaMetricsMigration - in case you decide to switch to one of the most popular LTS - solutions - VictoriaMetrics, as there is no officially supported available migration tool, this code will help you do this automatically

**You'll find all the installation requirements as well as usage cases under each corresponding folder**