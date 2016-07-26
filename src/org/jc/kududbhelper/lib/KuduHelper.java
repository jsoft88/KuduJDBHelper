/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jc.kududbhelper.lib;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;
import com.google.gson.reflect.TypeToken;
import com.stumbleupon.async.Deferred;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.kududb.ColumnSchema;
import org.kududb.Common;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.AsyncKuduScanner;
import org.kududb.client.AsyncKuduSession;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduPredicate;
import org.kududb.client.KuduScanner;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.OperationResponse;
import org.kududb.client.PartialRow;
import org.kududb.client.RowResult;
import org.kududb.client.RowResultIterator;
import org.kududb.client.SessionConfiguration;

/**
 *
 * @author cespedjo
 *
 */
public class KuduHelper {

    private static final short MUTATION_BUFFER_SIZE = 10000;

    private static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy/MM/dd hh:mm:ss";

    private String tableName;

    private List<String> kuduMasterAddresses;

    private Deferred<KuduTable> asyncTable;

    private KuduTable syncTable;

    private KuduClient syncClient;

    private AsyncKuduClient asyncClient;

    private KuduSession syncSession;

    private AsyncKuduSession asyncSession;

    public static final short KUDU_SESSION_SYNC = 0;

    public static final short KUDU_SESSION_ASYNC = 1;

    /**
     *
     * @param tableName name of the table to work on.
     * @param kuduMasters one or more available master addresses.
     * @throws Exception when tableName or master addresses are empty or null.
     */
    public KuduHelper(String tableName, String[] kuduMasters)
            throws java.lang.Exception {
        if (tableName == null || kuduMasters == null
                || tableName.isEmpty() || kuduMasters.length == 0) {
            throw new Exception("Kudu Table or Master address cannot be null nor empty.");
        }
        this.tableName = tableName;
        this.kuduMasterAddresses = Arrays.asList(kuduMasters);
    }

    /**
     *
     * @param sessionType Indicates whether the session to be established will
     * be synchronous or asynchronous. For more information on AsyncKuduSession:
     * http://getkudu.io/releases/0.5.0/apidocs/org/kududb/client/AsyncKuduSession.html
     * For more information on Synchronous client:
     * http://getkudu.io/apidocs/org/kududb/client/KuduClient.html
     * @param flushMode expects one of the flush modes. For more information:
     * http://getkudu.io/releases/0.5.0/apidocs/org/kududb/client/SessionConfiguration.FlushMode.html
     * @throws Exception when sessionType is invalid.
     */
    public synchronized void init(final short sessionType, final SessionConfiguration.FlushMode flushMode) throws Exception {

        if (sessionType == KUDU_SESSION_SYNC) {
            this.syncClient = new KuduClient.KuduClientBuilder(this.kuduMasterAddresses).build();
            this.asyncClient = null;
            this.syncTable = this.syncClient.openTable(this.tableName);
            this.syncSession = this.syncClient.newSession();
            this.syncSession.setFlushMode(flushMode);
            this.syncSession.setTimeoutMillis(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
            this.syncSession.setMutationBufferSpace(MUTATION_BUFFER_SIZE);
        } else if (sessionType == KUDU_SESSION_ASYNC) {
            this.asyncClient = new AsyncKuduClient.AsyncKuduClientBuilder(this.kuduMasterAddresses).build();
            this.syncClient = null;
            this.asyncTable = this.asyncClient.openTable(this.tableName);
            this.asyncSession = this.asyncClient.newSession();
            this.asyncSession.setFlushMode(flushMode);
            this.asyncSession.setTimeoutMillis(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
            this.asyncSession.setMutationBufferSpace(MUTATION_BUFFER_SIZE);
        }
    }

    private void addColumnValue(PartialRow row, String columnName, Schema schema,
            Object value) throws Exception {
        ColumnSchema cs = schema.getColumn(columnName);
        if (cs == null) {
            throw new Exception("Invalid column name: " + columnName);
        }

        //Default value for null
        value = (value == null ? "0" : value);
        if (cs.getType() == Type.STRING) {
            row.addString(columnName, String.valueOf(value));
        } else if (cs.getType() == Type.BINARY) {
            row.addBinary(columnName, (byte[]) value);
        } else if (cs.getType() == Type.BOOL) {
            row.addBoolean(columnName, Boolean.parseBoolean(String.valueOf(value)));
        } else if (cs.getType() == Type.DOUBLE) {
            row.addDouble(columnName, Double.parseDouble(String.valueOf(value)));
        } else if (cs.getType() == Type.FLOAT) {
            row.addFloat(columnName, Float.parseFloat(String.valueOf(value)));
        } else if (cs.getType() == Type.INT16 || cs.getType() == Type.INT8) {
            row.addShort(columnName, Short.parseShort(String.valueOf(value)));
        } else if (cs.getType() == Type.INT32) {
            row.addInt(columnName, Integer.parseInt(String.valueOf(value)));
        } else if (cs.getType() == Type.INT64) {
            row.addLong(columnName, Long.parseLong(String.valueOf(value)));
        } else if (cs.getType() == Type.TIMESTAMP) {
            //Convert to long millis
            row.addLong(columnName, new SimpleDateFormat(DEFAULT_TIMESTAMP_FORMAT)
                    .parse(String.valueOf(value)).getTime());
        } else {
            throw new Exception("Invalid format: " + cs.getType().getName()
                    + " when inserting value " + String.valueOf(value) + " in column:"
                    + columnName);
        }
    }

    private void processObjectsAsMap(List<Map<String, String>> data)
            throws InterruptedException, IllegalStateException, Exception {
        if (this.syncSession == null && this.asyncSession == null) {
            throw new Exception("Invalid session: null. Did you forget to invoke init()?");
        }

        KuduTable auxTable = this.syncTable == null
                ? this.asyncTable.join()
                : this.syncTable;

        final Schema tableSchema = auxTable.getSchema();

        for (int i = 0; i < data.size(); ++i) {
            Map<String, String> aRow = data.get(i);
            final Insert insert = auxTable.newInsert();
            PartialRow row = insert.getRow();
            for (Map.Entry<String, String> entry : aRow.entrySet()) {
                this.addColumnValue(row, entry.getKey(), tableSchema, entry.getValue());
            }

            if (this.syncSession != null) {
                this.syncSession.apply(insert);
                if (this.isTimeToFlush()) {
                    this.flushRecordsInsertionSyncSession();
                }
            } else if (this.asyncSession != null) {
                this.asyncSession.apply(insert);
                if (this.isTimeToFlush()) {
                    this.flushRecordsInsertionAsyncSession();
                }
            } else {
                throw new Exception("Invalid kudu session.");
            }
        }

        if (this.syncSession != null) {
            this.flushRecordsInsertionSyncSession();
        } else if (this.asyncSession != null) {
            this.flushRecordsInsertionAsyncSession();
        } else {
            throw new Exception("Invalid kudu session.");
        }
    }

    public void processMap(List<Map<String, String>> data)
            throws InterruptedException, IllegalStateException, Exception {
        this.processObjectsAsMap(data);
    }

    /**
     * Method to process data in a json array. Usually invoked when invoking
     * from cli commands. Notice that when KuduHelper is included as an external
     * jar inside a given project, this is not the preferred method.
     *
     * @param jsonArray an array of json objects with the format
     * Map<String, String>
     * @throws InterruptedException This exception indicates that an async kudu
     * table did not successfully joined within timeout millis.
     * @throws Exception An exception when inserting data, parsing values to be
     * inserted or invalid data types.
     */
    public void processJson(String jsonArray)
            throws InterruptedException, IllegalStateException, Exception {
        GsonBuilder gsonBuilder = new GsonBuilder();
        java.lang.reflect.Type type = new TypeToken<List<Map<String, String>>>() {
        }.getType();
        gsonBuilder.setLongSerializationPolicy(LongSerializationPolicy.STRING);
        Gson gson = gsonBuilder.create();
        List<Map<String, String>> data = gson.fromJson(jsonArray, type);
        if (data.isEmpty()) {
            throw new Exception("Cannot process an empty data list.");
        }

        this.processObjectsAsMap(data);
    }

    /**
     * Overloaded version of processData(JsonArray), normally used when this jar
     * is included inside another project.
     *
     * @param rows List of objects instantiated from class extending BaseRow.
     * @throws Exception An exception when inserting data, parsing values to be
     * inserted or invalid data types.
     * @throws InterruptedException This exception indicates that an async kudu
     * table did not successfully joined within timeout millis.
     */
    public void processPojos(List<BaseRow> rows)
            throws Exception, InterruptedException {
        KuduTable auxTable = this.syncTable == null
                ? this.asyncTable.join()
                : this.syncTable;

        final Schema tableSchema = auxTable.getSchema();

        if (rows == null || rows.isEmpty()) {
            throw new Exception("Cannot process an empty data list.");
        }

        for (BaseRow aRow : rows) {
            final Insert insert = auxTable.newInsert();
            PartialRow row = insert.getRow();
            for (Field aField : aRow.getClass().getDeclaredFields()) {
                if (BaseRow.isIgnoreField(aField.getName())) {
                    continue;
                }

                aField.setAccessible(true);
                this.addColumnValue(row, aField.getName(), tableSchema, aField.get(aRow));
            }

            if (this.syncSession != null) {
                this.syncSession.apply(insert);
                if (this.isTimeToFlush()) {
                    this.flushRecordsInsertionSyncSession();
                }
            } else if (this.asyncSession != null) {
                this.asyncSession.apply(insert);
                if (this.isTimeToFlush()) {
                    this.flushRecordsInsertionAsyncSession();
                }
            } else {
                throw new Exception("Invalid kudu session.");
            }
        }

        if (this.syncSession != null) {
            //When dataset has been fully processed flush.
            this.flushRecordsInsertionSyncSession();
        } else if (this.asyncSession != null) {
            //When dataset has been fully processed flush.
            this.flushRecordsInsertionAsyncSession();
        } else {
            throw new Exception("Invalid kudu session.");
        }
    }

    private List<OperationResponse> flushRecordsInsertionSyncSession() throws Exception {
        return this.syncSession.flush();
    }

    private Deferred<List<OperationResponse>> flushRecordsInsertionAsyncSession()
            throws Exception {
        return this.asyncSession.flush();
    }

    /**
     * This method must always be overriden. When flush mode isn't set to
     * MANUAL, it should always return false. If it is set to MANUAL, then apply
     * some technique to flush operation in table.
     *
     * @return
     */
    private boolean isTimeToFlush() {
        return false;
    }

    private Schema getTableSchema() throws Exception {
        KuduTable table = null;
        if (this.syncSession != null) {
            table = this.syncClient.openTable(this.tableName);
        } else if (this.asyncSession != null) {
            table
                    = this.asyncClient.openTable(this.tableName)
                    .joinUninterruptibly();
        } else {
            throw new Exception("KuduHelper missing initialization. "
                    + "Invoke init method after getting an instance through factory");
        }

        return table.getSchema();
    }

    public List<Map<String, Type>> getColumnsTypePair() throws Exception {
        List<Map<String, Type>> retVal = new ArrayList<>();

        Schema schema = this.getTableSchema();

        for (ColumnSchema colSchema : schema.getColumns()) {
            Map<String, Type> aPair = new HashMap<>();
            aPair.put(colSchema.getName(), colSchema.getType());

            retVal.add(aPair);
        }

        return retVal;
    }

    public List<Map<String, Type>> getPrimaryKeyColumns() throws Exception {
        List<Map<String, Type>> retVal = new ArrayList<>();

        Schema schema = this.getTableSchema();
        for (ColumnSchema colSchema : schema.getColumns()) {
            if (!colSchema.isKey()) {
                continue;
            }

            Map<String, Type> aPair = new HashMap<>();
            aPair.put(colSchema.getName(), colSchema.getType());

            retVal.add(aPair);
        }

        return retVal;
    }

    private KuduPredicate getPredicate(BaseFilter aFilter, KuduTable table) {
        ColumnSchema cs = table.getSchema().getColumn(aFilter.getColumnName());
        KuduPredicate p = null;
        if (cs.getType().getDataType() == Common.DataType.DOUBLE) {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), (double) aFilter.getValue());
        } else if (cs.getType().getDataType() == Common.DataType.BOOL) {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), (boolean) aFilter.getValue());
        } else if (cs.getType().getDataType() == Common.DataType.INT16
                || cs.getType().getDataType() == Common.DataType.INT32
                || cs.getType().getDataType() == Common.DataType.INT8) {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), (int) aFilter.getValue());
        } else if (cs.getType().getDataType() == Common.DataType.INT64) {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), (long) aFilter.getValue());
        } else if (cs.getType().getDataType() == Common.DataType.STRING) {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), aFilter.getValue().toString());
        } else if (cs.getType().getDataType() == Common.DataType.BINARY) {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), aFilter.getBinaryValue());
        } else {
            p = KuduPredicate
                    .newComparisonPredicate(cs, aFilter.getOperator(), (float) aFilter.getValue());
        }
        
        return p;
    }

    /**
     * Filter data in table by applying the filters provided.
     * @param filters a List containing filters to be applied. Notice that filters
     * will be AND-ed.
     * @return a list of RowResult or null if there are no results.
     * @throws Exception If anything goes wrong.
     */
    public List<RowResult> queryData(List<BaseFilter> filters) throws Exception {
        KuduScanner scanner = null;
        AsyncKuduScanner asyncScanner = null;
        KuduTable table = null;

        if (this.syncClient != null) {
            table = this.syncClient.openTable(this.tableName);
            scanner = this.syncClient.newScannerBuilder(table).build();
        } else if (this.asyncClient != null) {
            table
                    = this.asyncClient.openTable(this.tableName)
                    .joinUninterruptibly();
            asyncScanner = this.asyncClient.newScannerBuilder(table).build();
        } else {
            throw new Exception("KuduHelper missing initialization. "
                    + "Invoke init method after getting an instance through factory");
        }
        AsyncKuduScanner.AsyncKuduScannerBuilder b = null;
        KuduScanner.KuduScannerBuilder sb = null;
        List<RowResult> resultSet = null;
        if (scanner != null) {
            sb = this.syncClient.newScannerBuilder(table);
            for (BaseFilter aFilter : filters) {
                sb.addPredicate(this.getPredicate(aFilter, table));
            }
            KuduScanner ks = sb.build();
            while (ks.hasMoreRows()) {
                if (resultSet == null) {
                    resultSet = new ArrayList<>();
                }
                resultSet.addAll(this.iterateResultSet(ks.nextRows()));
            }
            ks.close();
            this.syncClient.close();
        } else if (asyncScanner != null) {
            b = this.asyncClient.newScannerBuilder(table);
            for (BaseFilter aFilter : filters) {
                b.addPredicate(this.getPredicate(aFilter, table));
            }
            AsyncKuduScanner aks = b.build();
            while (aks.hasMoreRows()) {
                if (resultSet == null) {
                    resultSet = new ArrayList<>();
                }
                    
                resultSet.addAll(this.iterateResultSet(aks.nextRows().join()));
            }
            aks.close();
            this.asyncClient.close();
        }
        
        return resultSet;
    }
    
    private List<RowResult> iterateResultSet(RowResultIterator rit) {
        List<RowResult> resultSet = null;
        while (rit.hasNext()) {
            if (resultSet == null) {
                resultSet = new ArrayList<>();
            }
            RowResult row = rit.next();
            resultSet.add(row);
        }
        return resultSet;
    }
}
