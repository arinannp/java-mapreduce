package covid;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MeanDeaths {
    public static void main(String args[]) {
        System.out.println("Total Mean Death Per Country 31/12/2019 - 14/12/2020");

        JobClient clientJob = new JobClient();
        // Create a configuration object for the job
        JobConf conf = new JobConf(MeanDeaths.class);

        // Set a name of the Job
        conf.setJobName("DeathsMeanPerCountry");

        // Specify data type of output key and value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        conf.setMapperClass(MapperCovid.class);
        conf.setReducerClass(ReducerCovid.class);
        conf.setCombinerClass(ReducerCovid.class);

        // Specify formats of the data type of Input and output
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Run the Job
        clientJob.setConf(conf);
        try {
            JobClient.runJob(conf);
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }
}
