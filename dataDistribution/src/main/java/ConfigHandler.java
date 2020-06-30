

public interface ConfigHandler {
	
	String SOURCE_FORMAT = "spark.source.format";
	String SOURCE_PATH = "spark.source.path";
	String SOURCE_OPTIONS_STRING = "spark.source.options.string";
	String SOURCE_OPTIONS_SEPARATOR = "spark.source.options.separator";
	String SOURCE_OPTIONS_KV_SEPARATOR = "spark.source.options.keyValue.separator";
	
	String INPUT_KEY = "spark.source.columns.key";
	String INPUT_MULTIKEYS_SPLITPATTERN = "spark.source.columns.multikeys.split"; // not yet used

	/* IF THE DATASOURCE IS HIVE TABLE */
	
	String HIVE_INPUT_DATABASE = "spark.hive.input.db";

	String HIVE_INPUT_QUERY = "spark.hive.input.query";
	String HIVE_INPUT_SEARCH_KEY = "spark.hive.input.key";
	String HIVE_INPUT_MULTIPLE_SEARCH_KEYS = "spark.hive.input.multiple.keys"; // future feature
	String HIVE_INPUT_KEYS_SPLITPATTERN = "spark.hive.input.keys.splitpattern"; // not yet used

	/* HBASE OUTPUT CONFIGURATION */

	String HBASE_OUTPUT_TABLE = "spark.hbase.output.table";
	String HBASE_OUTPUT_TABLE_COLUMN_FAMILY = "spark.hbase.output.table.column.family";
	String HBASE_HFILE_PARTITIONER_NBPARTITIONS = "spark.hbase.partitioner.partitions";

	/* SPARK PERFORMANCE */

	String SPARK_COALEASE_NUM_PARTITIONS = "spark.coalease.partitions";

	/* KAFKA RELATED */

	String KAFKA_BOOTSTRAP_SERVER = "spark.kafka.bootstrap.server";
	String KAFKA_TOPIC = "spark.kafka.topic";
	String KAFKA_CLIENT_JAAS = "spark.kafka.jaas.file";
	String KAFKA_CONSUMER_GROUP = "spark.kafka.consumer.group";

}
