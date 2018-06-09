package ru.sberbank.bigdata.graph.cassandra;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import scala.Tuple2;
import scala.Tuple4;
import scala.collection.JavaConversions;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class CsvReader implements Serializable {
    private static final char QUOTE_AND_ESCAPE_CHAR = '\"';
    private static final char DELIMITER_CHAR = ';';
    private static final int INPUT_SCHEMA_SIZE = 7;
    private static final String dateFormat = "yyyy-MM-dd";
    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern(dateFormat)
            .toFormatter();

    private transient final JavaSparkContext jsc;
    private transient final SQLContext sqlContext;
    private final String inputPath;

    public CsvReader(JavaSparkContext jsc, SQLContext sqlContext, String inputPath) {
        this.jsc = jsc;
        this.sqlContext = sqlContext;
        this.inputPath = inputPath;
    }

    public DataFrame readAndGroup() {

        StructType keyStruct = new StructType()
                .add("start_id", StringType, false)
                .add("end_id", StringType, false);
        StructType csvStruct = new StructType()
                .add("start_id", StringType, false)
                .add("dr", LongType, false)
                .add("end_id", StringType, false)
                .add("cr", LongType, false)
                .add("description", StringType)
                .add("summ", LongType)
                .add("tdate", StringType);

        final List<String> types = new ArrayList<>();
        for (StructField field : JavaConversions.asJavaIterable(csvStruct)) {
            types.add(field.dataType().typeName());
        }

        final StructType connectionStructType = new StructType()
                .add("dr", LongType)
                .add("cr", LongType)
                .add("description", StringType)
                .add("summ", LongType)
                .add("tdate", StringType);
        StructType finalStruct = keyStruct.add("connection_metas", createArrayType(connectionStructType));
        JavaRDD<Row> conversionResult = jsc
                .newAPIHadoopFile(inputPath, TextInputFormat.class, LongWritable.class, Text.class, jsc.hadoopConfiguration())
                .map(new ParserFunction(types));

        JavaPairRDD<Tuple2<String, String>, Iterable<Row>> grouppedResult = conversionResult
                .filter(new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row v1) throws Exception {
                        return v1 != null;
                    }
                })
                .groupBy(new Function<Row, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2<>(
                                row.getString(0),
                                row.getString(2));
                    }
                });
        JavaRDD<Row> rdd = grouppedResult
                .map(new Function<Tuple2<Tuple2<String, String>, Iterable<Row>>, Row>() {
                    @Override
                    public Row call(Tuple2<Tuple2<String, String>, Iterable<Row>> tuple) throws Exception {
                        List<Row> newRows = new ArrayList<>();
                        for (Row oldRow : tuple._2()) {
                            newRows.add(RowFactory.create(oldRow.get(1), oldRow.get(3), oldRow.get(4), oldRow.get(5), oldRow.get(6)));
                        }
                        return RowFactory.create(tuple._1()._1(),
                                tuple._1()._2(),
                                JavaConversions.asScalaIterable(newRows).toList());
                    }
                });
        return sqlContext.createDataFrame(rdd, finalStruct);
    }

    private static class ParserFunction implements Function<Tuple2<LongWritable, Text>, Row> {
        private static final ThreadLocal<CsvParser> csvParser;

        static {
            CsvFormat csvFormat = new CsvFormat();
            csvFormat.setDelimiter(DELIMITER_CHAR);
            final CsvParserSettings csvParserSettings = new CsvParserSettings();
            csvParserSettings.setFormat(csvFormat);
            csvParser = new ThreadLocal<CsvParser>() {
                @Override
                protected CsvParser initialValue() {
                    return new CsvParser(csvParserSettings);
                }
            };
        }

        private final List<String> types;

        ParserFunction(List<String> types) {
            this.types = types;
        }

        @Override
        public Row call(Tuple2<LongWritable, Text> tuple) throws Exception {
            if (tuple._1().get() > 0) {
                String row = tuple._2().toString();
                String[] tokens = csvParser.get().parseLine(row);
                if (tokens == null || tokens.length < INPUT_SCHEMA_SIZE) {
                    return null;
                } else {
                    try {
                        Object[] cells = new Object[INPUT_SCHEMA_SIZE];
                        for (int i = 0; i < INPUT_SCHEMA_SIZE; i++) {
                            cells[i] = convert(types.get(i), tokens[i]);
                        }
                        return RowFactory.create(cells);
                    } catch (Exception e) {
                        return null;
                    }
                }
            }
            return null;
        }

        private Object convert(String typeName, String value) {
            if ("string".equals(typeName)) {
                return value;
            } else if ("long".equals(typeName)) {
                return value == null ? null : Long.parseLong(value);
            } else if ("date".equals(typeName)) {
                return new Date(formatter.parseDateTime(value).getMillis());
            } else {
                throw new IllegalArgumentException("type was not found");
            }
        }
    }

    private static class Tuple2Comparator implements Comparator<Tuple2<String, String>>, Serializable {
        @Override
        public int compare(Tuple2<String, String> o1, Tuple2<String, String> o2) {
            int firstCmp = o1._1().compareTo(o2._1());
            if(firstCmp == 0) {
                return o1._2().compareTo(o2._2());
            }
            return firstCmp;
        }
    }
}
