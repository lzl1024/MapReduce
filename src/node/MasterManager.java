package node;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import dfs.FileSplit;

/**
 * 
 * Management tool
 * 
 */
public class MasterManager extends Thread {

    @Override
    public void run() {
        String cmdInput = "";
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        printUsage();

        while (!cmdInput.equals("quit")) {
            System.out.print("cmd% ");
            try {
                cmdInput = in.readLine();
                String[] cmd = cmdInput.split(" ");

                if (cmd.length == 1) {
                    if (cmdInput.equals("files")) {
                        System.out.println(FileSplit.splitLayout);
                    } else if (cmdInput.equals("jobs")) {
                        System.out.println(Scheduler.jobPool);
                    } else if (cmdInput.equals("slaves")) {
                        System.out.println(MasterMain.slavePool);
                    }
                } else if (cmd.length == 2 && cmd[0].equals("kjob")) {
                    int jobID = Integer.parseInt(cmd[1]);
                    
                    if (!Scheduler.jobPool.containsKey(jobID)) {
                        System.out.println("No such job is running!");
                        continue;
                    }
                    
                    synchronized (Scheduler.killedJob) {
                        Scheduler.killedJob.add(jobID);
                    }
                }
            } catch (Exception e) {
                System.out.println("Invalid Input!");
            }
        }
        System.exit(0);
    }

    private void printUsage() {
        System.out.println("Usage: 'quit' to quit the master");
        System.out.println("'files' : show the layout of file distribution");
        System.out.println("'jobs' : show jobs infomation");
        System.out.println("'slaves' : show slaves infomation");
        System.out.println("'kjob <jobID>' : kill one job");
    }
}
