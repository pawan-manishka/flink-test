package com.input;

import org.apache.flink.core.io.InputSplit;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse.Entry;

import java.util.List;

/**
 * Created by pawan on 2/6/17.
 */
public class DASInputSplit implements InputSplit {

    private Entry entry;
    private List<Entry> entries;
    private int noOfEntries;

    public DASInputSplit(List<Entry> entries,int noOfEntries) {
        this.entries = entries;
        this.noOfEntries = noOfEntries;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    // This will return the no of splits that is equal to no of entries
    public int getSplitNumber() {
        return noOfEntries;
    }

}
