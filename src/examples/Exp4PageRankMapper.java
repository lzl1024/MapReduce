package examples;

import io.Context;
import io.LongWritable;
import io.Text;

import java.util.ArrayList;
import java.util.Arrays;

import mapreduce.Mapper;

public class Exp4PageRankMapper extends Mapper {
    @Override
    public void map(LongWritable key, Text value, Context context) {
        String line = value.get();
        String[] elements = line.split("\\s+");
        ArrayList<String> companys = new ArrayList<String>(Arrays.asList(
                "Yahoo", "Facebook", "Google", "Tintri", "Amazon"));
        if(elements.length != 4) {
        	return;
        }
        LongWritable pageRank = new LongWritable(Long.parseLong(elements[2]));
        for (String company : companys) {
            if (elements[1].startsWith(company)) {
                context.write(new Text(company), pageRank);
                break;
            }
        }
    }
}
