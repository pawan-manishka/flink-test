package com.input;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse.Entry;
import org.wso2.carbon.analytics.dataservice.core.AnalyticsServiceHolder;
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
    private AnalyticsIterator<Record> iterator;
    private int tenantId;
    private String tableName;
    private int numPartitionsHint;
    private List<String> columns;
    private long timeFrom;
    private long timeTo;
    private int recordsFrom;
    private int recordsCount;

    public DASInputFormat(int tenantId, String tableName, int numPartitionsHint, List<String> columns,
                          long timeFrom, long timeTo, int recordsFrom, int recordsCount) {
        this.tenantId = tenantId;
        this.tableName = tableName;
        this.columns = columns;
        this.numPartitionsHint = numPartitionsHint;
        this.timeFrom = timeFrom;
        this.timeTo = timeTo;
        this.recordsFrom = recordsFrom;
        this.recordsCount = recordsCount;
    }

    public void configure(Configuration parameters) {
        // do nothing here
    }

    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }
    /*
     * This method decides the no of splits need to be created
     */
    public DASInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        // get the entries and create an array using that
        try {
            analyticsDataResponse = AnalyticsServiceHolder.getAnalyticsDataService().get(tenantId, tableName, numPartitionsHint,
                    columns, timeFrom, timeTo, recordsFrom, recordsCount);
        } catch (AnalyticsException e) {
            e.printStackTrace();
        }
        List<Entry> entries = analyticsDataResponse.getEntries();
        DASInputSplit[] splitsArr = new DASInputSplit[entries.size()];

        for (int i = 0; i < splitsArr.length; i++) {
            splitsArr[i] = new DASInputSplit(entries.get(i), i);
        }
        return splitsArr;
    }

    public InputSplitAssigner getInputSplitAssigner(DASInputSplit[] inputSplits) {
        System.err.println("split assigner");
        return new DefaultInputSplitAssigner(inputSplits);
    }

    /*
     * Read records in Input split
     */
    public void open(DASInputSplit split) throws IOException {
        if (split != null) {
            System.err.println("open split");
            RecordGroup recordGroup = split.getEntry().getRecordGroup();
            String recordStore = split.getEntry().getRecordStoreName();
            try {
                iterator = AnalyticsServiceHolder.getAnalyticsDataService().readRecords(recordStore, recordGroup);
            } catch (AnalyticsException e) {
                e.printStackTrace();
            }
        } else {
            throw new IllegalArgumentException("open() failed");
        }
    }
    /*
     * Checks whether all data has been read.
     */
    public boolean reachedEnd() throws IOException {
        System.err.println("end: " + iterator.hasNext());
        return (!iterator.hasNext());
    }

    public Record nextRecord(Record reuse) throws IOException {
        return iterator.next();
    }

    public void close() throws IOException {
    }

}
