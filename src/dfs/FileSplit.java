package dfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * 
 * Class to split the file in the distributed file system
 *
 */
public class FileSplit {

    /**
     * split the file according to its file name and replication factor
     * @param fileName
     * @param replFac
     * @return file split names
     * @throws IOException
     */
    public static String[] splitFile(String fileName, int replFac) throws IOException{
        File file = new File(fileName);
        PrintWriter[] pwList = new PrintWriter[replFac];
        String[] splitNames = new String[replFac];
        
        if (!file.exists()) {
            throw new IOException("File cannot found");
        }
        
        //get split names
        for (int i=1; i<=replFac; i++) {
            splitNames[i-1] = fileName+i;
            pwList[i-1] = new PrintWriter(new FileWriter(splitNames[i-1]), true);
        }
        
        //split file
        int i = 0;
        BufferedReader read = new BufferedReader(new FileReader(fileName));
        String record;
        while((record = read.readLine()) != null){
            pwList[i].println(record);
            i = (i + 1) % replFac;
        }
        
        //close files
        read.close();
        for (PrintWriter pw: pwList) {
            pw.close();
        }

        return splitNames;
    }

    //for test
    public static void main(String[] args) throws IOException{
        splitFile("src/fs/story1.txt",2);
    }

}
