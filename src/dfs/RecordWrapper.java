package dfs;

import java.io.Serializable;

public class RecordWrapper implements Serializable {

    private static final long serialVersionUID = 1L;


    private Long recordNum;
    private String fileName;
    
    public RecordWrapper(Long recordNum, String fileName) {
        this.recordNum = recordNum;
        this.fileName = fileName;
    }
    
    public Long getRecordNum() {
        return recordNum;
    }

    public void setRecordNum(Long recordNum) {
        this.recordNum = recordNum;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
}