

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;

import com.google.protobuf.ServiceException;
import core.ApplicationContextJava;
import core.ConfigHandler;
import exceptions.MissingKeysException;
import propertiesValidator.HivePropertiesValidator;
import propertiesValidator.IPropertiesValidator;
import utils.Utils;

import scala.Tuple2;
import scala.collection.Seq;

public class HiveToHBaseJob {

	/**
	 * 
	 * @param args
	 * @throws MasterNotRunningException
	 * @throws ZooKeeperConnectionException
	 * @throws ServiceException
	 * @throws IOException
	 * @throws MissingKeysException
	 * @throws SQLException
	 */
	public static void main(String[] args)
			throws MasterNotRunningException, ZooKeeperConnectionException, ServiceException, IOException, MissingKeysException, SQLException {

		ApplicationContextJava app = ApplicationContextJava.getInstance();
		Logger logger = app.getLogger();

		SparkSession spark = app.getSparkSession(false);
		SparkConf sparkConf = spark.sparkContext().getConf();

		IPropertiesValidator properties = new HivePropertiesValidator().build(sparkConf);
		if (!properties.areValidProperties())
			throw new MissingKeysException();

		spark.catalog().setCurrentDatabase(sparkConf.get(ConfigHandler.HIVE_INPUT_DATABASE));

		/*
		 * Please make sure to add a sorting clause to the input query. Otherwise,
		 * instructions are given below when dealing with the PairRDD.
		 * 
		 * It's better when dealt with from Hive side. Please consider also taking a
		 * look at the clauses "CLUSTER BY column" and
		 * "DISTRIBUTE BY column1 SORT BY column2". When are they equivalent and when it
		 * has a sense to differentiate between the two of them ?
		 */
		Dataset<Row> queryResultDS = spark.sql(sparkConf.get(ConfigHandler.HIVE_INPUT_QUERY));

		/*
		 * Retrieving and sorting the columns as HFile requirement
		 */
		List<String> columns = Arrays.asList(queryResultDS.columns());
		columns.sort((x, y) -> x.compareTo(y));

		/*
		 * Since We need to sort the columns, I need to store the search key new column
		 * index to create the HBase rowKey. This will be used later !
		 */
		String searchKey = sparkConf.get(ConfigHandler.HIVE_INPUT_SEARCH_KEY);

		if (!columns.contains(searchKey))
			throw new SQLException(searchKey + " column not found among (" + columns.stream().collect(Collectors.joining(", ")) + ")");

		int searchKeyIndex = columns.indexOf(searchKey);

		@SuppressWarnings("unchecked")
		scala.collection.Seq<Column> SortedColumns = (Seq<Column>) Utils.convertListToSeq(columns.stream().map(colName -> new Column(colName)).collect(Collectors.toList()));

		JavaRDD<Row> rdd = queryResultDS.select(SortedColumns).toJavaRDD();

		/*
		 * Preparing the job
		 */

		Configuration configuration = app.getHBaseConfiguration();
		Job job = Job.getInstance(configuration);

		String tableName = sparkConf.get(ConfigHandler.HBASE_OUTPUT_TABLE);

		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String pathToHFile = tableName + "_" + timestamp.getTime();

		Connection connection = ConnectionFactory.createConnection(configuration);

		Table table = connection.getTable(TableName.valueOf(tableName));
		RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));

		HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

		JavaPairRDD<ImmutableBytesWritable, KeyValue> rddToStore = rdd.flatMapToPair(row -> {

			ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>> columnQualifiers = new ArrayList<Tuple2<ImmutableBytesWritable, KeyValue>>();

			byte[] rowKey = Bytes.toBytes(row.get(searchKeyIndex).toString());

			for (String columnName : columns) {

				int currentColumnIndex = row.fieldIndex(columnName);
				String currentValue = row.isNullAt(currentColumnIndex) ? "" : row.get(currentColumnIndex).toString();

				String columnFamily = sparkConf.get(ConfigHandler.HBASE_OUTPUT_TABLE_COLUMN_FAMILY);
				KeyValue kv = new KeyValue(rowKey, Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), timestamp.getTime(), Bytes.toBytes(currentValue));

				columnQualifiers.add(new Tuple2<ImmutableBytesWritable, KeyValue>(new ImmutableBytesWritable(rowKey), kv));
			}
			return columnQualifiers.iterator();
		});

		/*
		 * If the input query does not contain any sorting clause, please make sure to
		 * enable one of the two followings. It is up to you to choose. I didn't made my mind yet.
		 */

//		This is bad, it implies heavy shuffle ! but worth testing on small data sets.
//		rddToStore.sortByKey();

//		This can be a good solution if the sorting is not made at the input query result
//		rddToStore.repartitionAndSortWithinPartitions(new HPartitioner(Integer.parseInt(sparkConf.get(ConfigHandler.HBASE_HFILE_PARTITIONER_NBPARTITIONS))));

		/*
		 * Finally store the sorted JavaPairRDD as HFiles for bulk loading!
		 */
		rddToStore.saveAsNewAPIHadoopFile(pathToHFile, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, job.getConfiguration());

		/*
		 * Load the HFiles into the corresponding table and clean working directory.
		 */
		LoadIncrementalHFiles loader = null;
		try {
			loader = new LoadIncrementalHFiles(job.getConfiguration());
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		Path hdfsPathToHFiles = new Path(pathToHFile);
		loader.doBulkLoad(hdfsPathToHFiles, connection.getAdmin(), table, regionLocator);
		app.getHDFS(spark).delete(hdfsPathToHFiles, true);

		connection.close();
	}

}
