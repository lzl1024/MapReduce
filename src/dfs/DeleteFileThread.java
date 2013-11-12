package dfs;

import java.io.File;
import java.io.FilenameFilter;

import util.Constants;

/**
 * 
 * Delete all of the file in the file system based on the file prefix
 *
 */
public class DeleteFileThread extends Thread {
    private String fileName;

    public DeleteFileThread(String fileName) {
        this.fileName = fileName;
    }

    public void run() {
        File dir = new File(Constants.FS_LOCATION);
        MyFilter filter = new MyFilter(fileName);
        String[] Flist = dir.list(filter);
        for (String file : Flist) {
            new File(Constants.FS_LOCATION + file).delete();
            System.out.println(file + " is deleted");
        }
    }

    private class MyFilter implements FilenameFilter {
        private String prefix;

        public MyFilter(String prefix) {
            this.prefix = prefix;
        }

        public boolean accept(File dir, String name) {
            return name.startsWith(prefix);
        }
    }

}
