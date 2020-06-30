

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;

import com.socgen.daas.utils.Utils;

/**
 * 
 * @author X165473
 *
 */
public interface IPropertiesValidator {

	/**
	 * This method build an instance of the implementing classes based on its fields
	 * @return an IPropertiesValidator
	 */
	IPropertiesValidator build(SparkConf sparkConf);

	/**
	 * This method will use the implementing class fields to create a row and then
	 * it uses the default function to validate it.
	 * 
	 * @return whether the properties row is valid.
	 */
	boolean areValidProperties();

	/**
	 * 
	 * @param row
	 * @return whether a given row is valid
	 */
	default boolean isValid(Row row) {
		return (row.anyNull() || Utils.anyEmpty(row));
	}

}
