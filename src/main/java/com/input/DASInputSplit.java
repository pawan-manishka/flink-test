package com.input;

import org.apache.flink.core.io.InputSplit;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse.Entry;


/**
 * Created by pawan on 2/6/17.
 */
public class DASInputSplit implements InputSplit {

    private Entry entry;
    private int entryNo;

    public DASInputSplit(Entry entry, int entryNo) {
        this.entry = entry;
        this.entryNo = entryNo;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    // This will return the no of splits that is equal to no of entries
    public int getSplitNumber() {
        return entryNo;
    }

}
