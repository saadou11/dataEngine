package utils;

import java.util.List;

import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConverters;

public class Utils {

	static Logger logger = LoggerFactory.getLogger(Utils.class);

	/**
	 * 
	 * @param inputList
	 * @return
	 */
	public static scala.collection.Seq<?> convertListToSeq(List<?> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	/**
	 * 
	 * @param row
	 * @return whether a given row contains any data type other than strings
	 */
	private static boolean rowOfStrings(Row row) {

		int i = 0;
		while (i < row.length())
			if (row.get(i) instanceof String) {
				i++;
				continue;
			} else
				return false;

		return true;
	}

	/**
	 * 
	 * @param row
	 * @return
	 */
	public static boolean anyEmpty(Row row) {

		if (!rowOfStrings(row)) {
			logger.error("row containing other than string fields");
			return false;
		}

		int i = 0;
		while (i < row.length()) {
			if (row.getString(i).isEmpty())
				return true;
			i++;
		}

		return false;
	}
}
