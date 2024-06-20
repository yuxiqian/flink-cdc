<p align="center">
  <a href="https://nightlies.apache.org/flink/flink-cdc-docs-stable/"><img src="docs/static/fig/flinkcdc-logo.png" alt="Flink CDC" style="width: 375px;"></a>
</p>
<p align="center">
<a href="https://github.com/apache/flink-cdc/" target="_blank">
    <img src="https://img.shields.io/github/stars/apache/flink-cdc?style=social&label=Star&maxAge=2592000" alt="Test">
</a>
<a href="https://github.com/apache/flink-cdc/releases" target="_blank">
    <img src="https://img.shields.io/github/v/release/apache/flink-cdc?color=yellow" alt="Release">
</a>
<a href="https://github.com/apache/flink-cdc/actions/workflows/flink_cdc.yml" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/apache/flink-cdc/flink_cdc.yml?branch=master" alt="Build">
</a>
<a href="https://github.com/apache/flink-cdc/tree/master/LICENSE" target="_blank">
    <img src="https://img.shields.io/static/v1?label=license&message=Apache License 2.0&color=white" alt="License">
</a>
</p>


Flink CDC is a distributed data integration tool for real time data and batch data. Flink CDC brings the simplicity 
and elegance of data integration via YAML to describe the data movement and transformation in a 
[Data Pipeline](docs/content/docs/core-concept/data-pipeline.md).


The Flink CDC prioritizes efficient end-to-end data integration and offers enhanced functionalities such as 
full database synchronization, sharding table synchronization, schema evolution and data transformation.

![Flink CDC framework desigin](docs/static/fig/architecture.png)

### Installation

* Flink CDC tar could be downloaded from [Apache Flink Website](https://flink.apache.org/downloads/#apache-flink-cdc) or [GitHub Release Page](https://github.com/apache/flink-cdc/releases).
* Pipeline and source connectors could be downloaded from [Maven Central Repository](https://mvnrepository.com/artifact/org.apache.flink) or [GitHub Release Page](https://github.com/apache/flink-cdc/releases).
* If you're using Linux or macOS, you may install Flink CDC and connectors with Homebrew:

```bash
brew install apache-flink-cdc
```

### Getting Started

1. Prepare a [Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/local_installation/#starting-and-stopping-a-local-cluster) cluster and set up `FLINK_HOME` environment variable.
2. [Download](https://github.com/apache/flink-cdc/releases) Flink CDC tar, unzip it and put jars of pipeline connector to Flink `lib` directory.
3. Create a **YAML** file to describe the data source and data sink, the following example synchronizes all tables under MySQL app_db database to Doris :
  ```yaml
    source:
       type: mysql
       name: MySQL Source
       hostname: 127.0.0.1
       port: 3306
       username: admin
       password: pass
       tables: adb.\.*
       server-id: 5401-5404
    
    sink:
      type: doris
      name: Doris Sink
      fenodes: 127.0.0.1:8030
      username: root
      password: pass
      
    transform:
    - source-table: adb.web_order01
      projection: \*, UPPER(product_name) as product_name
      filter: id > 10 AND order_id > 100
      description: project fields and filter
    - source-table: adb.web_order02
      projection: \*, UPPER(product_name) as product_name
      filter: id > 20 AND order_id > 200
      description: project fields and filter  

    route:
    - source-table: adb.web_order\.*
      sink-table: adb.ods_web_orders
      description: sync sharding tables to one destination table
    
    pipeline:
       name: MySQL to Doris Pipeline
       parallelism: 4
  ```
4. Submit pipeline job using `flink-cdc.sh` script.
 ```shell
  bash bin/flink-cdc.sh /path/mysql-to-doris.yaml
 ```
5. View job execution status through Flink WebUI or downstream database.

Try it out yourself with our more detailed [tutorial](docs/content/docs/get-started/quickstart/mysql-to-doris.md). 
You can also see [connector overview](docs/content/docs/connectors/pipeline-connectors/overview.md) to view a comprehensive catalog of the
connectors currently provided and understand more detailed configurations.



### Join the Community

There are many ways to participate in the Apache Flink CDC community. The
[mailing lists](https://flink.apache.org/what-is-flink/community/#mailing-lists) are the primary place where all Flink
committers are present. For user support and questions use the user mailing list. If you've found a problem of Flink CDC,
please create a [Flink jira](https://issues.apache.org/jira/projects/FLINK/summary) and tag it with the `Flink CDC` tag.   
Bugs and feature requests can either be discussed on the dev mailing list or on Jira.



### Contributing

Welcome to contribute to Flink CDC, please see our [Developer Guide](docs/content/docs/developer-guide/contribute-to-flink-cdc.md)
and [APIs Guide](docs/content/docs/developer-guide/understand-flink-cdc-api.md).



### License

[Apache 2.0 License](LICENSE).



### Special Thanks

The Flink CDC community welcomes everyone who is willing to contribute, whether it's through submitting bug reports,
enhancing the documentation, or submitting code contributions for bug fixes, test additions, or new feature development.     
Thanks to all contributors for their enthusiastic contributions.

<a href="https://github.com/apache/flink-cdc/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=apache/flink-cdc"/>
</a>
