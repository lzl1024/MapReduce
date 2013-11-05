package socket;

import java.io.Serializable;
import java.util.ArrayList;

import util.Constants;

import mapreduce.Job;

public class ReducerAckMsg implements Serializable {

    /**
     * Acknowledge reducer message
     */
    private static final long serialVersionUID = 1L;

    private int mapperNum;
    private String ReducerClass;
    private ArrayList<String> fileNames;
    private int jobID;
    private int index;
    private Class<?> reduceKey;
    private Class<?> reduceValue;

    public ReducerAckMsg(int mapperNum, Job job, int index) {
        this.mapperNum = mapperNum;
        this.ReducerClass = job.getReducerClass();
        this.jobID = job.getJobID();
        this.index = index;
        this.reduceKey = job.getReducerKeyClass();
        this.reduceValue = job.getReducerValueClass();

        this.fileNames = new ArrayList<String>();
        for (int i = 1; i <= mapperNum; i++) {
            this.fileNames.add(Constants.FS_LOCATION + job.getJobID() + "_"
                    + job.getInputFile() + "_" + i + "_" + index);
        }
    }

    public int getMapperNum() {
        return mapperNum;
    }

    public void setMapperNum(int mapperNum) {
        this.mapperNum = mapperNum;
    }

    public String getReducerClass() {
        return ReducerClass;
    }

    public void setReducerClass(String reducerClass) {
        ReducerClass = reducerClass;
    }

    public ArrayList<String> getfileNames() {
        return this.fileNames;
    }

    public int getJobID() {
        return jobID;
    }

    public void setJobID(int jobID) {
        this.jobID = jobID;
    }
    
    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Class<?> getReduceKey() {
        return reduceKey;
    }

    public void setReduceKey(Class<?> reduceKey) {
        this.reduceKey = reduceKey;
    }

    public Class<?> getReduceValue() {
        return reduceValue;
    }

    public void setReduceValue(Class<?> reduceValue) {
        this.reduceValue = reduceValue;
    }
    public String toString() {
    	return this.ReducerClass + " " + this.mapperNum + " " + this.jobID + " " + this.index + " " + this.fileNames;
    }
}
