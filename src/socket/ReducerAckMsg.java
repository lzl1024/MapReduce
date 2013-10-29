package socket;

import java.io.Serializable;
import java.util.ArrayList;

import mapreduce.Job;

public class ReducerAckMsg implements Serializable {

    /**
     * Acknowledge reducer message
     */
    private static final long serialVersionUID = 1L;

    private int mapperNum;
    private String ReducerClass;
    private ArrayList<String> fileNames;

    public ReducerAckMsg(int mapperNum, Job job, int index) {
        this.mapperNum = mapperNum;
        this.ReducerClass = job.getReducerClass();

        this.fileNames = new ArrayList<String>();
        for (int i = 1; i <= mapperNum; i++) {
            this.fileNames.add(job.getJobID() + "_" + job.getInputFile() + "_"
                    + i + "_" + index);
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
}
