

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.protobuf.ServiceException;
import com.socgen.cgu.ApplicationContext;

@Service
public class ConnectionHandler {

	Logger logger = LoggerFactory.getLogger(ApplicationContext.class);

	@Value("${hive.uri.2}")
	private String databaseUri_2;

	@Value("${hive.uri.1000}")
	private String databaseUri_1000;

	@Value("${hive.driver}")
	private String driver;

	@Value("${cluster.hbase.conf}")
	private String hbaseSite;

	@Value("${cluster.core.conf}")
	private String coreSite;

	@Value("${krb5.conf}")
	private String krb5ConfFile;

	@Value("${krb5.principal}")
	private String krb5Principal;

	@Value("${krb5.keytab}")
	private String krb5Keytab;

	java.sql.Connection jdbcConnection = null;
	org.apache.hadoop.hbase.client.Connection hBaseConnection = null;

	/**
	 * Authentication using KRB credentials
	 */
	private void authenticate() {

		System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
		System.setProperty("java.security.krb5.conf", krb5ConfFile);

		Configuration conf = new Configuration();
		conf.set("hadoop.security.authentication", "kerberos");
		UserGroupInformation.setConfiguration(conf);

		try {
			UserGroupInformation.loginUserFromKeytab(krb5Principal, krb5Keytab);
		} catch (IOException e1) {
			logger.error(e1.getMessage());
		}

	}

	/**
	 * @return HiveJDBC connection or null if exceptions occurs
	 * @throws IOException
	 */
	public Connection getConnection(String databaseUri) {

		if (jdbcConnection == null) {

			authenticate();

			try {
				Class.forName(driver);
				jdbcConnection = (Connection) java.sql.DriverManager.getConnection(databaseUri, "", "");
			} catch (ClassNotFoundException e) {
				logger.error(e.getMessage());
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}

		return jdbcConnection;
	}

	/**
	 * 
	 * @return
	 */
	public org.apache.hadoop.hbase.client.Connection getHBaseConnection() {

		authenticate();

		Configuration conf = HBaseConfiguration.create();

		/*
		 * THIS WORK ONLY IF CONFIG FILES ARE IN THE JAR ! NEED TO CHANGE THIS FOR FILES
		 * OUTSIDE WHEN DEPLOYING IN CI
		 */

		String hbaseSitePath = this.getClass().getClassLoader().getResource(hbaseSite).getPath();
		String coreSitePath = this.getClass().getClassLoader().getResource(coreSite).getPath();

		conf.addResource(new Path(hbaseSitePath));
		conf.addResource(new Path(coreSitePath));

		/*
		 * =============================================================================
		 */

		if (hBaseConnection == null) {
			
			try {
				hBaseConnection = ConnectionFactory.createConnection(conf);
			} catch (IOException e1) {
				logger.error(e1.getMessage());
			}

			try {
				HBaseAdmin.checkHBaseAvailable(conf);
			} catch (MasterNotRunningException e) {
				logger.error(e.getMessage());
			} catch (ZooKeeperConnectionException e) {
				logger.error(e.getMessage());
			} catch (ServiceException e) {
				logger.error(e.getMessage());
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		}

		return hBaseConnection;
	}
}
