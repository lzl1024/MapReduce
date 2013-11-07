package socket;

import java.io.Serializable;

public class RecordWrapperMsg implements Serializable {

    private static final long serialVersionUID = 1L;


    private Long recordNum;
    private String fileName;
    
    public RecordWrapperMsg(Long recordNum, String fileName) {
        this.recordNum = recordNum;
        this.fileName = fileName;
    }
    
    public Long getRecordNum() {
        return recordNum;
    }

    public void setRecordNum(Long recordNum) {
        this.recordNum = recordNum;
    }

    @Override
    public String toString() {
        return "RecordWrapperMsg [recordNum=" + recordNum + ", fileName="
                + fileName + "]";
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
}