package ru.sberbank.bigdata.graph.cassandra;

import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableList;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created by georgii on 08.06.18.
 */
public class BulkSSTableCreator implements org.apache.spark.api.java.function.Function2<Integer, java.util.Iterator<org.apache.spark.sql.Row>, java.util.Iterator<Object>> {
    private static final String TYPE_NAME = "meta_v2";
    private final String outputDir;
    /**
     * Keyspace name
     */
    private final String keyspace;
    /**
     * Table name
     */
    private final String table;

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    private final String schema;
    private final String insertStmt;
    private final String metaType;

    public BulkSSTableCreator(String outputDir, String keyspace, String table) {
        this.outputDir = outputDir;
        this.keyspace = keyspace;
        this.table = table;
        this.metaType = String.format("CREATE TYPE %s.%s (dr bigint, cr bigint, description text, summ bigint, tdate text)", keyspace, TYPE_NAME);
        this.schema = String.format("CREATE TABLE %s.%s (" +
                "start_id string, " +
                "end_id string, " +
                "connections_meta list<frozen<%s>>" +
                "PRIMARY KEY ((start_id, end_id))) ", keyspace, table, TYPE_NAME);
        this.insertStmt = String.format("INSERT INTO %s.%s (" +
                "start_id, end_id, connections_meta" +
                ") VALUES (" +
                "?, ?, ?" +
                ")", keyspace, table);
    }

    @Override
    public Iterator<Object> call(Integer i, Iterator<Row> rows) throws Exception {
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir + i)
                .withType(metaType)
                .forTable(schema)
                .using(insertStmt)
                .withPartitioner(new Murmur3Partitioner());

        try(CQLSSTableWriter writer = builder.build();) {
            UserType udt = writer.getUDType(TYPE_NAME);
            for (Iterator<Row> it = rows; it.hasNext(); ) {
                Row row = it.next();
                List<UDTValue> metas = new ArrayList<>();
                List<Row> list = row.getList(2);
                for (Row meta : list) {
                    metas.add(udt.newValue()
                            .setLong(0, meta.isNullAt(0) ? -1 : meta.getLong(0))
                            .setLong(1, meta.isNullAt(1) ? -1 : meta.getLong(1))
                            .setString(2, meta.getString(2))
                            .setLong(3, meta.isNullAt(2) ? -1 : meta.getLong(2))
                            .setString(4, meta.getString(4)));
                }
                writer.addRow(row.getString(0), row.getString(1), metas);
            }
        }

        return Collections.emptyIterator();
    }
}
