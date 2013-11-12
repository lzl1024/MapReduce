package examples;

import io.IntWritable;
import io.Text;
import mapreduce.Job;

/**
 * 
 * Example 1: Do word count for one file
 *
 */
public class Exp1WordCountMain {

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("args length" + args.length);
            System.out
                    .println("Useage: Exp1WordCountMain <config file> <input path> <output path>");
            System.exit(1);
        }

        // configure job object
        Job job = new Job();
        job.setJobName("WordCount");
        job.setInputFile(args[1]);
        //job.setOutputFile(args[2]);

        job.setMapperClass("Exp1WordCountMapper");
        job.setReducerClass("Exp1WordCountReducer");
        job.setReducerKeyClass(Text.class);
        job.setReducerValueClass(IntWritable.class);
        job.setRecordBegin(10L);
        job.setRecordEnd(10000L);

        try {
            job.waitForCompletion(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
