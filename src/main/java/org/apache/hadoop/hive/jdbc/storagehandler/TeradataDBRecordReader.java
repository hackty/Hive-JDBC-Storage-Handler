package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBRecordReader;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.hadoop.hive.jdbc.storagehandler.Constants.DEFAULT_INPUT_FETCH_SIZE;
import static org.apache.hadoop.hive.jdbc.storagehandler.Constants.INPUT_FETCH_SIZE;

/**
 * A RecordReader that reads records from an TeraData SQL table.
 */
public class TeradataDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

    private DBInputFormat.DBInputSplit split;

    final Configuration conf;
    private static final Log LOG = LogFactory.getLog(TeradataDBRecordReader.class);

    public TeradataDBRecordReader(DBInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table)
            throws SQLException {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
        this.conf = conf;
    }

    @Override
    protected String getSelectQuery() {
        StringBuilder query = new StringBuilder();
        DBConfiguration dbConf = getDBConf();
        String conditions = getConditions();
        String tableName = getTableName();
        String [] fieldNames = getFieldNames();
        if (dbConf.getInputQuery() == null) {
            query.append("SELECT ");

            try {
                if(null != split && split.getLength()>0){
                    query.append(" TOP ").append(split.getLength()).append(" ");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            for(int i = 0; i < fieldNames.length; ++i) {
                query.append(fieldNames[i]);
                if (i != fieldNames.length - 1) {
                    query.append(", ");
                }
            }

            query.append(" FROM ").append(tableName);
            query.append(" AS ").append(tableName);
            if (conditions != null && conditions.length() > 0) {
                query.append(" WHERE (").append(conditions).append(")");
            }

            String orderBy = dbConf.getInputOrderBy();
            if (orderBy != null && orderBy.length() > 0) {
                query.append(" ORDER BY ").append(orderBy);
            }
        } else {
            query.append(dbConf.getInputQuery());
        }

        return query.toString();
    }

    @Override
    protected ResultSet executeQuery(String query) throws SQLException {
        this.statement = getConnection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(conf.getInt(INPUT_FETCH_SIZE, DEFAULT_INPUT_FETCH_SIZE));
        return statement.executeQuery();
    }

    @Override
    public void close() throws IOException {
        try {
            statement.cancel();
        } catch (SQLException e) {
            // Ignore any errors in cancelling, this is not fatal
            LOG.error("Could not cancel query: "  + this.getSelectQuery());
        }
//        to keep connection
//        super.close();
    }
}
