package core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ServiceException;

public class ApplicationContextJava {

    private ApplicationContextJava() {}

    public static ApplicationContextJava getInstance() {
        if (app == null)
            app = new ApplicationContextJava();

        return app;
    }

    /**
     *
     * @return
     */
    public Logger getLogger() {
        if (logger == null)
            logger = LoggerFactory.getLogger(ApplicationContextJava.class);

        return logger;
    }

    /**
     *
     * @return
     */
    public SparkSession getSparkSession(Boolean withKyro) {

        Builder builder = SparkSession.builder();
        SparkConf sparkConf = new SparkConf();

        if (withKyro) {

            builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            Class<?>[] classes = new Class[] { org.apache.hadoop.hbase.io.ImmutableBytesWritable.class };

            sparkConf.registerKryoClasses(classes);
        }

        builder.config(sparkConf);

        spark = builder.enableHiveSupport().getOrCreate();
        return spark;
    }

    /**
     *
     * @param spark
     * @return
     */
    public JavaSparkContext getJavaSparkContext(SparkSession spark) {

        if (jspark == null)
            jspark = new JavaSparkContext(spark.sparkContext());

        return jspark;
    }

    /**
     *
     * @return
     * @throws ServiceException
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public Configuration getHBaseConfiguration() throws ServiceException, MasterNotRunningException, ZooKeeperConnectionException, IOException {

        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin.checkHBaseAvailable(configuration);

        return configuration;
    }


    /**
     *
     * @param spark
     * @return HADOOP configuration
     * @throws IOException
     */
    public FileSystem getHDFS(SparkSession spark) throws IOException {
        return FileSystem.get(app.getJavaSparkContext(spark).hadoopConfiguration());
    }

    private static ApplicationContextJava app = null;
    private static SparkSession spark = null;
    private static JavaSparkContext jspark = null;
    private static Logger logger = null;
}