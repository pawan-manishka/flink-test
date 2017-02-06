package com.wso2.test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.testng.annotations.Test;
import com.input.DASInputFormat;
import static org.testng.Assert.*;

/**
 * Created by pawan on 2/6/17.
 */
public class DASInputFormatTest {
    private DASInputFormat dasInputFormat;
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
    public void fullTest() throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        dasInputFormat=DASInputFormat.buildDASInputFormat().setTenantId(11).setTableName("smwsi").setNumPartitionsHint(0)
                .setColumns(null).setTimeFrom(1480574985).setTimeTo(1480576985).setRecordsFrom(10).setRecordsCount(20);
      //  environment.execute();
    }
}