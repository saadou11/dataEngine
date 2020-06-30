

import org.apache.spark.SparkConf;
import org.apache.spark.sql.RowFactory;

import com.socgen.daas.applicationContext.ConfigHandler;

/**
 * 
 * This class is used to validate the mandatory properties of the Hive To HBase
 * job. Properties must be set in the application.properties when submitting
 * spark job
 *
 */
public class HivePropertiesValidator implements IPropertiesValidator {

	private String database;
	private String query;
	private String key;

	public HivePropertiesValidator() {
	}

	private HivePropertiesValidator withHiveDB(String database) {
		this.database = database;
		return this;
	}

	private HivePropertiesValidator withQuery(String query) {
		this.query = query;
		return this;
	}

	private HivePropertiesValidator withKey(String key) {
		this.key = key;
		return this;
	}

	/*
	 * Method to test in local
	 */
	public IPropertiesValidator localBuild() {

		IPropertiesValidator propertiesValidator = new HivePropertiesValidator().withHiveDB("DB").withQuery("QUERY").withKey("KEY");

		return propertiesValidator;
	}

	@Override
	public IPropertiesValidator build(SparkConf sparkConf) {

		IPropertiesValidator propertiesValidator = new HivePropertiesValidator().withHiveDB(sparkConf.get(ConfigHandler.HIVE_INPUT_DATABASE))
				.withQuery(sparkConf.get(ConfigHandler.HIVE_INPUT_QUERY)).withKey(sparkConf.get(ConfigHandler.HIVE_INPUT_SEARCH_KEY));

		return propertiesValidator;
	}

	@Override
	public boolean areValidProperties() {
		return this.isValid(RowFactory.create(this.database, this.query, this.key));
	}

}
