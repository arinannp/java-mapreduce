package covid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class ReducerCovid extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        int totalDeaths = 0;
        int count = 0;

        while (values.hasNext()) {
            IntWritable value = (IntWritable) values.next();
            totalDeaths += value.get();
            count++;
        }

        int meanDeaths = totalDeaths / count;
        output.collect(key, new IntWritable(meanDeaths));
    }
}
