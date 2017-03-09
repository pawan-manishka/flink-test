package com.wso2.test;

import com.input.DASInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import org.wso2.carbon.analytics.dataservice.core.AnalyticsDataService;
import org.wso2.carbon.analytics.dataservice.core.AnalyticsServiceHolder;
import org.wso2.carbon.analytics.datasource.commons.*;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsException;
import org.wso2.carbon.analytics.datasource.core.rs.AnalyticsRecordStore;
import org.wso2.carbon.analytics.datasource.core.util.GenericUtils;

import javax.naming.NamingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.wso2.carbon.analytics.datasource.core.AnalyticsRecordStoreTest.generateRecords;

/**
 * Created by pawan on 2/3/17.
 */
public class FlinkTest {

    private AnalyticsDataService service;
    private AnalyticsIterator<Record> iterator;
    private AnalyticsRecordStore analyticsRS;
    RecordGroup recordGroup;
    String recordstore;

    @BeforeClass
    public void setup() throws NamingException, AnalyticsException, IOException {
        GenericUtils.clearGlobalCustomDataSourceRepo();
        System.setProperty(GenericUtils.WSO2_ANALYTICS_CONF_DIRECTORY_SYS_PROP, "src/test/resources/conf1");
        AnalyticsServiceHolder.setHazelcastInstance(null);
        AnalyticsServiceHolder.setAnalyticsClusterManager(null);
        System.setProperty(AnalyticsServiceHolder.FORCE_INDEXING_ENV_PROP, Boolean.TRUE.toString());
        service = AnalyticsServiceHolder.getAnalyticsDataService();
    }

    @AfterClass
    public void done() throws NamingException, AnalyticsException, IOException {

        System.clearProperty(AnalyticsServiceHolder.FORCE_INDEXING_ENV_PROP);
        AnalyticsServiceHolder.setAnalyticsDataService(null);
    }

    @Test
    public void testCreateInputSplits() throws Exception {
    }

    @Test
    public void testOpen() throws Exception {
    }

    @Test
    public void testReachedEnd() throws Exception {
    }


    @Test
    public void createTable() throws Exception {

        // create a table and putting sample data
        int tenantId = 1;
        String table1 = "SampleTable1";
        String table2 = "T2";

        service.createTable(tenantId, table1);
        service.createTable(tenantId, table2);
        List<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
        columns.add(new ColumnDefinitionExt("STR1", AnalyticsSchema.ColumnType.STRING, true, false));
        columns.add(new ColumnDefinitionExt("STR2", AnalyticsSchema.ColumnType.STRING, true, false));

        Map<String, Object> values = new HashMap<String, Object>();
        values.put("STR1", "Smith");
        values.put("STR2", "Steve");

        Map<String, Object> values2 = new HashMap<String, Object>();
        values2.put("STR1", "Pawan");
        values2.put("STR2", "Nuwan");

        Map<String, Object> values3 = new HashMap<String, Object>();
        values3.put("STR1", "Kohli");
        values3.put("STR2", "Gayle");

        List<String> tables = service.listTables(1); // tenantId = 1

        Record record = new Record(tenantId, table1, values);
        Record record2 = new Record(tenantId, table1, values2);
        Record record3 = new Record(tenantId, table1, values3);

        List<Record> records = new ArrayList<Record>();
        records.add(record);
        records.add(record2);
        records.add(record3);

        service.setTableSchema(tenantId, table1, new AnalyticsSchema(columns, null));
        service.put(records);

        // executing the flink input format
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        DASInputFormat dasInputFormat = new DASInputFormat(1, "SAMPLETABLE1", 2, null, Long.MIN_VALUE, Long.MAX_VALUE, 0, -1);
        DataSet<Record> dasRecords = environment.createInput(dasInputFormat);
        List<Record> res = dasRecords.collect();


        System.out.println("*XXXXXXXX: " + res.size());
        dasRecords.print();

        service.createTable(7, "Output1");
        long time = System.currentTimeMillis();
        int timeOffset = 10;
        List<Record> records1 = generateRecords(7, "Output1", 2, 200, time, timeOffset);
        AnalyticsServiceHolder.getAnalyticsDataService().put(records1);

       /* DASOutputFormat dasOutputFormat = new DASOutputFormat(records);
        dasRecords.output(dasOutputFormat);*/

        //AnalyticsServiceHolder.getAnalyticsDataService().put(res);


        // long i=dasRecords.count();
        //dasRecords.count();
        //environment.execute();
        System.out.println("dasReords execution");
        service.deleteTable(tenantId, table1);
        service.deleteTable(tenantId, table2);

        /*
        AnalyticsDataResponse analyticsDataResponse = AnalyticsServiceHolder.getAnalyticsDataService().get(1,"SampleTable1",1, null ,Long.MIN_VALUE, Long.MAX_VALUE, 0,-1 );
        for (AnalyticsDataResponse.Entry entry : analyticsDataResponse.getEntries()) {
            recordGroup = entry.getRecordGroup();
            recordstore = entry.getRecordStoreName();
        }
        iterator = AnalyticsServiceHolder.getAnalyticsDataService().readRecords(recordstore,recordGroup);
        int count =0;
        while(iterator.hasNext()) {
           System.out.println(iterator.next().toString());
            //System.out.println(iterator.next().getValues());
            count ++;
        }
        System.out.println(count);*/
    }
}
