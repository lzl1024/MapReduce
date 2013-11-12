package examples;

import io.LongWritable;
import io.Text;
import mapreduce.Job;

/**
 * 
 * Example 4: Given a large wikipedia pagerank file and get the pagerank of some
 * companys' websites.
 *
 */
public class Exp4PageRankMain {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("args length" + args.length);
            System.out
                    .println("Useage: Exp4PageRankMain <config file> <input path> <output path>");
            System.exit(1);
        }

        // configure job object
        Job job = new Job();
        job.setJobName("PageRank");
        job.setInputFile(args[1]);
        job.setOutputFile(args[2]);

        job.setMapperClass("Exp4PageRankMapper");
        job.setReducerClass("Exp4PageRankReducer");
        job.setReducerKeyClass(Text.class);
        job.setReducerValueClass(LongWritable.class);


        try {
            job.waitForCompletion(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
