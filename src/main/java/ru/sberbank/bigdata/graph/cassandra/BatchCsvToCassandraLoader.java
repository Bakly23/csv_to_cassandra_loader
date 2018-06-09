package ru.sberbank.bigdata.graph.cassandra;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class BatchCsvToCassandraLoader {
    public static void main(String[] args) {
        if (args.length != 6) {
            throw new IllegalArgumentException();
        }
        SparkConf sparkConf = new SparkConf()
                .setAppName("Csv to cassandra loader")
                .set("spark.cassandra.connection.host", args[1]);
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf);) {
            SQLContext context = new SQLContext(jsc);
            DataFrame df = new CsvReader(jsc, context, args[0]).readAndGroup();

            df.javaRDD().mapPartitionsWithIndex(new BulkSSTableCreator(args[5], args[3], args[4]), true).count();
        }

    }
}
