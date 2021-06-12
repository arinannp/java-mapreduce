package covid;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class MapperCovid extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        String valueString = value.toString();
        String[] arrayCovidData = valueString.split(",");

        if (!(arrayCovidData[6].equals("countriesAndTerritories")) && !(arrayCovidData[5].equals("deaths"))) {
            int deaths = Integer.parseInt(arrayCovidData[5]);
            output.collect(new Text(arrayCovidData[6]), new IntWritable(deaths));
        }
    }
}
