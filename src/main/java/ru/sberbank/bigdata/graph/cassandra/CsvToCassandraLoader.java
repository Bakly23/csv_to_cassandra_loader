package ru.sberbank.bigdata.graph.cassandra;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class CsvToCassandraLoader {
    public static void main(String[] args) {
        if (args.length != 6) {
            throw new IllegalArgumentException();
        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("Csv to cassandra loader")
                .set("spark.cassandra.connection.host", args[1])
                .set("spark.hadoop.hive.metastore.warehouse.dir", args[5])
                .set("spark.sql.warehouse.dir", args[5])
                .set("hive.metastore.warehouse.dir", args[5])
                .set("spark.hive.metastore.warehouse.dir", args[5]);
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf);) {
            SQLContext context = new SQLContext(jsc);
            DataFrame df = new CsvReader(jsc, context, args[0]).readAndGroup();
            df
                    .write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("cluster", args[2])
                    .option("keyspace", args[3])
                    .option("table", args[4])
                    .mode(SaveMode.Append)
                    .save();
        }

    }
}
