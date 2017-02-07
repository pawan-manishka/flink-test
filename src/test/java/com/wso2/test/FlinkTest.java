package com.wso2.test;

import com.input.DASInputFormat;
import junit.framework.Assert;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.wso2.carbon.analytics.dataservice.core.AnalyticsDataService;
import org.wso2.carbon.analytics.dataservice.core.AnalyticsServiceHolder;
import org.wso2.carbon.analytics.datasource.commons.Record;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsException;
import org.wso2.carbon.analytics.datasource.core.util.GenericUtils;

import javax.naming.NamingException;
import java.io.IOException;

/**
 * Created by pawan on 2/3/17.
 */
public class FlinkTest {

    private DASInputFormat dasInputFormat;
    private AnalyticsDataService service;

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
    public void myTest() throws Exception {
//        dasInputFormat.createInputSplits(1);

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        dasInputFormat= DASInputFormat.buildDASInputFormat().setTenantId(11).setTableName("smwsi").setNumPartitionsHint(0)
                .setColumns(null).setTimeFrom(1480574985).setTimeTo(1480576985).setRecordsFrom(10).setRecordsCount(20);

        DataSet<Record> sourceRecords = environment.createInput(DASInputFormat.buildDASInputFormat());
       // sourceRecords.print();
    }

}
