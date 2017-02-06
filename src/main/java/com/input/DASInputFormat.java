package com.input;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse.Entry;
import org.wso2.carbon.analytics.dataservice.core.AnalyticsDataService;
import org.wso2.carbon.analytics.datasource.commons.AnalyticsIterator;
import org.wso2.carbon.analytics.datasource.commons.Record;
import org.wso2.carbon.analytics.datasource.commons.RecordGroup;
import org.wso2.carbon.analytics.datasource.commons.exception.AnalyticsException;

import java.io.IOException;
import java.util.List;

/**
 * Created by pawan on 2/2/17.
 */
public class DASInputFormat implements InputFormat<Record, DASInputSplit> {

    private AnalyticsDataResponse analyticsDataResponse;
    private AnalyticsDataService analyticsDataService;
    private AnalyticsIterator<Record> iterator;


    private int tenantId;
    private String tableName;
    private int numPartitionsHint;
    private List<String> columns;
    private long timeFrom;
    private long timeTo;
    private int recordsFrom;
    private int recordsCount;

    boolean reachedEnd = false;


    public void configure(Configuration parameters) {
        // do nothing here
    }

    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    /*
    * This method checks numPartitonHint and decide the no of splits need to be created
    * */
    public DASInputSplit[] createInputSplits(int minNumSplits) throws IOException {
            /// get the entries and create an array using that
        try {
            analyticsDataResponse = analyticsDataService.get(tenantId, tableName, numPartitionsHint, columns, timeFrom, timeTo, recordsFrom, recordsCount);

        } catch (AnalyticsException e) {
            e.printStackTrace();
        }

        






        DASInputSplit s = new DASInputSplit("", minNumSplits);
        DASInputSplit[] dasSplits= new DASInputSplit[]
        for (int i = 0; i < dasSplits.length; i++) {
            dasSplits[i] = new DASInputSplit();
        }

        return dasSplits;


/*        if(numPartitionsHint==0){
            return new DASInputSplit[]{new DASInputSplit(entry)};
        }
        DASInputSplit[] dasSplits= new DASInputSplit[numPartitionsHint];
        for (int i = 0; i < dasSplits.length; i++) {
            dasSplits[i] = new DASInputSplit(entry);
        }

        return dasSplits;*/





       /* if (numPartitionsHint == 0) {
            return new DASInputSplit[]{new DASInputSplit(0, 1)};  // (noOfPartitions,totalNoOfPartitions)
        }
        DASInputSplit[] dasSplits = new DASInputSplit[numPartitionsHint];
        for (int i = 0; i < dasSplits.length; i++) {
            dasSplits[i] = new DASInputSplit(i, dasSplits.length);
        }
        return dasSplits;*/
    }

    public InputSplitAssigner getInputSplitAssigner(DASInputSplit[] inputSplits) {
        return null;
    }

    /*
     * Read records in Input split
     * */
    public void open(DASInputSplit split) throws IOException {
        try {
            analyticsDataResponse = analyticsDataService.get(tenantId, tableName, numPartitionsHint, columns, timeFrom, timeTo, recordsFrom, recordsCount);
            analyticsDataService.readRecords();
        } catch (AnalyticsException e) {
            e.printStackTrace();
        }
        for (Entry entry : analyticsDataResponse.getEntries()) {
            RecordGroup recordGroup = entry.getRecordGroup();
            String recordstore = entry.getRecordStoreName();
            try {
                iterator = analyticsDataService.readRecords(recordstore, recordGroup);
                iterator.next();
            } catch (AnalyticsException e) {
                e.printStackTrace();
            }
        }
    }

    /*
    * Checks whether all data has been read.
    * */
    public boolean reachedEnd() throws IOException {
        if (!iterator.hasNext()) {
            return false;
        }
        return true;
    }

    public Record nextRecord(Record reuse) throws IOException {
        return iterator.next();
    }

    public void close() throws IOException {

    }

    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static DASInputFormatBuilder buildDASInputFormat() {
        return new DASInputFormatBuilder();
    }

    public static class DASInputFormatBuilder extends DASInputFormat {
        // set those input values to com.input.DASInputFormat attributes
        private final DASInputFormat format;

        public DASInputFormatBuilder() {
            this.format = new DASInputFormat();
        }

        public DASInputFormatBuilder setTenantId(int tenantId) {
            format.tenantId = tenantId;
            return this;
        }

        public DASInputFormatBuilder setTableName(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public DASInputFormatBuilder setNumPartitionsHint(int numPartitionsHint) {
            format.numPartitionsHint = numPartitionsHint;
            return this;
        }

        public DASInputFormatBuilder setColumns(List<String> columns) {
            format.columns = columns;
            return this;
        }

        public DASInputFormatBuilder setTimeFrom(long timeFrom) {
            format.timeFrom = timeFrom;
            return this;
        }

        public DASInputFormatBuilder setTimeTo(long timeTo) {
            format.timeTo = timeTo;
            return this;
        }

        public DASInputFormatBuilder setRecordsFrom(int recordsFrom) {
            format.recordsFrom = recordsFrom;
            return this;
        }

        public DASInputFormatBuilder setRecordsCount(int recordsCount) {
            format.recordsCount = recordsCount;
            return this;
        }

    }
}
