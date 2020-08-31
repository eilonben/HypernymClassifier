package EMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MainPipeline {
    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        conf1.set("DPmin",args[3]);
        Job job1 = Job.getInstance(conf1, "Step1");
        job1.setJarByClass(Step1.class);
        job1.setMapperClass(Step1.MapperStep1.class);
        job1.setReducerClass(Step1.ReducerStep1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        if (job1.waitForCompletion(true))
            System.out.println("Step 1 Completed");
        else
            System.out.println("Step 1 Failed");

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Step2");
        job2.setJarByClass(Step2.class);
        job2.setMapperClass(Step2.MapperStep2.class);
        job2.setReducerClass(Step2.ReducerStep2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        if (job2.waitForCompletion(true))
            System.out.println("Step 2 Completed");
        else
            System.out.println("Step 2 Failed");
    }
}
