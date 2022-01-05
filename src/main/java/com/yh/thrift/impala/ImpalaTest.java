package com.yh.thrift.impala;

import com.yh.thrift.impala.utils.KerberosAuthenticate;
import com.yh.thrift.impala.utils.YHTUGIAssumingTransport;
import net.minidev.json.JSONObject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ImpalaTest {

    public static void main(String[] args) throws Exception{
        HashMap<String, Object> test03 = submitQuery("abcc.yhddddd.cn", 21050,
                "select null",
                "test03", null);

        System.out.println(JSONObject.toJSONString(test03));
    }



    public static HashMap<String, Object> submitQuery(String host, int port, String sql, String username, String password) throws Exception {
        TCLIService.Client client = kerberlizeImpalaThrift(username, host, port);
        TOpenSessionReq openReq = new TOpenSessionReq();
        openReq.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);
        openReq.setUsername(username);
        TOpenSessionResp openResp = client.OpenSession(openReq);
        TSessionHandle sessHandle = openResp.getSessionHandle();
        TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, sql);
        //设置异步
        execReq.setRunAsync(true);
        TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
        TStatus status = execResp.getStatus();
        TOperationHandle tOperationHandle = execResp.getOperationHandle();
        HashMap<String, Object> result = new HashMap<>();

        if (tOperationHandle == null) {
            String msg = execResp.getStatus().getErrorMessage();
            result.put("status", "error");
            result.put("data", msg);
        }

        try{
            boolean flag = true;
            while(flag){
                TOperationState queryHandleStatus = getQueryHandleStatus(client, tOperationHandle);
                if (queryHandleStatus == TOperationState.RUNNING_STATE) {
                    getQueryLog(client, tOperationHandle);
                    result.put("type", "log");
                    result.put("data", getQueryLog(client, tOperationHandle));
                } else if (queryHandleStatus == TOperationState.FINISHED_STATE) {
                    result.put("type", "result");
                    result.put("data", getResult(client, tOperationHandle, sql));
                    flag = false;
                } else if (status.getStatusCode() == TStatusCode.ERROR_STATUS) {
                    String msg = status.getErrorMessage();
                    result.put("type", "error");
                    result.put("data", msg);
                    flag = false;
                }
            }
            //关闭连接
            TCloseOperationReq closeReq = new TCloseOperationReq();
            closeReq.setOperationHandle(tOperationHandle);
            client.CloseOperation(closeReq);
            TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
            client.CloseSession(closeConnectionReq);
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }


    private static TOperationState getQueryHandleStatus(TCLIService.Client client, TOperationHandle tOperationHandle) throws Exception {
        TOperationState tOperationState = null;
        if (tOperationHandle != null) {
            TGetOperationStatusReq statusReq = new TGetOperationStatusReq(tOperationHandle);
            TGetOperationStatusResp statusResp = client.GetOperationStatus(statusReq);
            tOperationState = statusResp.getOperationState();
        }
        return tOperationState;
    }

    private static String getQueryLog(TCLIService.Client client, TOperationHandle tOperationHandle) throws Exception {
        String log = null;
        if (tOperationHandle != null) {
            TGetLogReq tGetLogReq = new TGetLogReq(tOperationHandle);
            TGetLogResp logResp = client.GetLog(tGetLogReq);
            log = logResp.getLog();
            System.out.println(log);
        }
        return log;
    }

    private static ArrayList<HashMap<String, Object>> getResult(TCLIService.Client client, TOperationHandle tOperationHandle, String sql) throws TException {
        //获取列名
        TGetResultSetMetadataReq metadataReq = new TGetResultSetMetadataReq(tOperationHandle);
        TGetResultSetMetadataResp metadataResp = client.GetResultSetMetadata(metadataReq);
        TTableSchema tableSchema = metadataResp.getSchema();
        ArrayList<String> columns = new ArrayList<>();
        if (tableSchema != null) {
            List<TColumnDesc> columnDescs = tableSchema.getColumns();
            for (TColumnDesc tColumnDesc : columnDescs) {
                columns.add(tColumnDesc.getColumnName());
            }
        }
        //获取数据
        TFetchResultsReq fetchReq = new TFetchResultsReq();
        fetchReq.setOperationHandle(tOperationHandle);
        fetchReq.setMaxRows(100);
        //org.apache.hive.service.cli.thrift.TFetchOrientation.FETCH_NEXT
        TFetchResultsResp resultsResp = client.FetchResults(fetchReq);
        TStatus status = resultsResp.getStatus();
        if (status.getStatusCode() == TStatusCode.ERROR_STATUS) {
            String msg = status.getErrorMessage();
            System.out.println(msg + "," + status.getSqlState() + "," + Integer.toString(status.getErrorCode()) + "," + status.isSetInfoMessages());
            System.out.println("After FetchResults: " + sql);
        }
        TRowSet resultsSet = resultsResp.getResults();
        List<TRow> resultRows = resultsSet.getRows();

        //判断转换值 当为null的时候自动转为false
        ArrayList<HashMap<String, Object>> retArray = new ArrayList<>();
        for (TRow resultRow : resultRows) {
            List<TColumnValue> row = resultRow.getColVals();
            List<Object> list_row = new ArrayList<>();
            for (TColumnValue field : row) {
                if (field.isSetStringVal()) {
                    list_row.add(field.getStringVal().getValue());
                } else if (field.isSetDoubleVal()) {
                    list_row.add(field.getDoubleVal().getValue());
                } else if (field.isSetI16Val()) {
                    list_row.add(field.getI16Val().getValue());
                } else if (field.isSetI32Val()) {
                    list_row.add(field.getI32Val().getValue());
                } else if (field.isSetI64Val()) {
                    list_row.add(field.getI64Val().getValue());
                } else if (field.isSetBoolVal()) {
                    list_row.add(field.getBoolVal().isValue());
                } else if (field.isSetByteVal()) {
                    list_row.add(field.getByteVal().getValue());
                }
            }
            HashMap<String, Object> map = new HashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                map.put(columns.get(i), list_row.get(i));
            }
            retArray.add(map);
        }
//        System.out.println(JSON.toJSON(retArray));
        return retArray;
    }


    private static TCLIService.Client kerberlizeImpalaThrift(String user, String host, int port)throws Exception {
        System.setProperty("java.security.krb5.conf","D:/yh/datagalaxy/uat_kerberos/krb5_uat.conf");

        HashMap<String, String> config = new HashMap<>(8);
        config.put("hadoop.security.authentication", "Kerberos");

        KerberosAuthenticate authenticate = KerberosAuthenticate.initKerberos(user,"D:/yh/datagalaxy/uat_kerberos/test03.keytab", config);

        UserGroupInformation ugi = authenticate.getUgi();
        TTransport transport = new TSocket(host, port);

        Map<String, String> saslProperties = new HashMap<>(8);
        TSaslClientTransport saslClientTransport = new TSaslClientTransport(
                // tell SASL to use GSSAPI, which supports Kerberos
                "GSSAPI",
                // authorizationid - null
                null,
                // kerberos primary for server
                "impala",
                // kerberos instance for server
                host,
                // Properties set, above
                saslProperties,
                // callback handler - null
                null,
                // underlying transport
                transport);

        YHTUGIAssumingTransport tugiAssumingTransport = new YHTUGIAssumingTransport(saslClientTransport, ugi);
        tugiAssumingTransport.open();

        TProtocol protocol = new TBinaryProtocol(tugiAssumingTransport);
        //connect to client todo: Async Client
        TCLIService.Client client = new TCLIService.Client(protocol);
        // saslClientTransport.open();
        return client;
    }

}
