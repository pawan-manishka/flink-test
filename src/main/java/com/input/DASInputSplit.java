package com.input;

import org.apache.flink.core.io.InputSplit;
import org.wso2.carbon.analytics.dataservice.commons.AnalyticsDataResponse.Entry;

/**
 * Created by pawan on 2/6/17.
 */
public class DASInputSplit implements InputSplit {

    private Entry entry;
    private int noOfSplits;

    public DASInputSplit(Entry entry , int noOfSplits) {
      this.entry = entry;
      this.noOfSplits = noOfSplits;
    }

    public Entry getEntry() {
        return entry;
    }

    public void setEntry(Entry entry) {
        this.entry = entry;
    }

    public int getSplitNumber() {
        return noOfSplits;
    }

}
