package examples;

import mapreduce.Job;

public class Exp1WordCountMain {

	public static void main(String[] args){
		if (args.length != 2) {
			System.out.println("Useage: Exp1WordCountMain <input path> <output path>");
			System.exit(1);
		}
		
		// configure job object
		Job job = new Job();
		job.setJobName("WordCount");
		job.setInputFile(args[1]);
		job.setOutputFile(args[2]);
		
		job.setMapperClass("Exp1WordCountMapper");
		job.setReducerClass("Exp1WordCountReducer");
		
		try {
			job.waitForCompletion(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
