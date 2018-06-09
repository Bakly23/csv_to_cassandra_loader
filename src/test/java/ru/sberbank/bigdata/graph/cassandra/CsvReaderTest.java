package ru.sberbank.bigdata.graph.cassandra;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.IOException;

public class CsvReaderTest {
    public static void main(String... args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Csv to cassandra loader")
                .setMaster("local[*]");
        FileUtils.deleteDirectory(new File("target/json"));
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf);) {
            SQLContext context = new SQLContext(jsc);
            DataFrame df = new CsvReader(jsc, context, "src/test/resources/input").readAndGroup();
            df.write().json("target/json");
        }
    }
}
