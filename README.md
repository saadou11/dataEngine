# Data Engine
This project consists of implementing data ingestion and distribution tools
### Compile
Before cloning project please make sure you have already instaled Apache Maven and then use the following command :
> mvn [clean] install

after doing this, 5 Jar files will be generated in every module folder
### Deploye
Once you have generated the libraries you can send them to the your sandbox or test it in your local if you have the required components already installed

### HDFS
before running the jar files please make sure to follow the following steps
- Copy all the files that you want to ingest into hdfs in the landing-area folder

*/landing-area/name_of_your_dataset_folder/data/all_your_data_files*
- add a metadata file in the path :

*/landing-area/name_of_your_dataset_folder/metadata.json*

The metadata file should contain the informations about your dataset

Following an exemple of how a metadata file should be :
```json
{

"targetDB" : "userdatadb",
"targetTableName" :"user_table",
"folderSrc" : "/tmp/user/",
"targetPath" : "/warehouse/",
"partitionColumn" : "",
"schema" : "(id INT, sexe STRING,taille INT)",

}
```

## Run Project
### eventProducer
before runing the event producer jar files make sure you have the following properties in your *application.properties*
```
spark.kafka.bootstrap.server=your_kafka_port
spark.kafka.auto.offset.reset=earliest
spark.kafka.enable.auto.commit=true
spark.kafka.topic=client
spark.kafka.consumer.group=
spark.kafka.current.offset=
spark.kafka.notification.topic=
spark.kafka.monitoring.topic=
```
and then run your librarie
>  ./eventProducer_launcher.sh

### ingestion-engine
to run the ingestion-engine process use the following command :
>  ./ingestion-engine_launcher.sh

### hiveToHbase
make sure to add this properties in application.properties file :
```
spark.hive.input.db=name_of_hive_database
spark.hive.input.query=query_to_get_data
spark.hive.input.key=primery_key
spark.hbase.output.table=name_of_hbase_table
spark.hbase.output.table.column.family=all_column_family
```
and then run :
>  ./hiveToHbase_launcher.sh

### data-distribution
to run the data-distribution process use the following command :
>  ./data-distribution_launcher.sh

after that you should consume your web services through Api like Swagger or Postman
