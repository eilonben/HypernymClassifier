package EMR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop
        .io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import sun.tools.jar.Main;
import utils.BiarcNode;
import utils.DepGraph;

public class Step1 {
    public static class MapperStep1 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable lineId, Text line, Context context)  throws IOException, InterruptedException {
            String[] lineSplit = line.toString().split("\t");
            String[] graph = lineSplit[1].split(" ");
            BiarcNode root = DepGraph.getGraphRoot(graph);
            if (root != null) {
                writeDepPath(root, "", root, context);
            }
        }

        public void writeDepPath(BiarcNode curr, String path, BiarcNode start, Context context) throws IOException, InterruptedException {
            if (curr.isNoun()) {
                if (path.equals("")) { // current node is the first noun in the path
                    path = curr.getTag();
                    for (BiarcNode c : curr.getChildren()) {
                        writeDepPath(c, path, curr, context);
                    }
                }
                else{ // current node is the 2nd noun in the path, so we write it down
                    path+= "-"+curr.getTag();
                    String words = start.getWord() + "#" + curr.getWord();
                    context.write(new Text(path),new Text(words));
                    writeDepPath(curr,"",curr,context);
                }
            }
            // current node is not a noun
            else{
                if(!path.equals("")){
                    path+= "-"+curr.getTag();
                }
                for (BiarcNode c : curr.getChildren()) {
                    writeDepPath(c, path, start, context);
                }
            }
        }
    }

    public static class ReducerStep1 extends Reducer<Text,Text,Text,Text> {
        private int dpMin = 10;
        private BufferedWriter bw1,bw2;
        private File depPaths,features;
        private int featuresCount;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            depPaths = new File("depPaths.txt");
            features = new File("features.txt");
            depPaths.createNewFile();
            features.createNewFile();
            bw1 = new BufferedWriter(new FileWriter(depPaths));
            bw2 = new BufferedWriter(new FileWriter(features));
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<Text> pairs = new HashSet<>();
            for(Text pair:values){
                pairs.add(pair);
                if(pairs.size()==dpMin){
                    break;
                }
            }
            if(pairs.size()<dpMin){
                return;
            }
            bw1.write(key.toString()+"\n");
            featuresCount++;
            for(Text pair: pairs){
                context.write(pair,key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            bw1.close();
            bw2.write(featuresCount + "\n");
            bw2.close();
            S3Handler = new S3Handler();
            S3Handler.upload(depPaths,"emr/depPaths.txt");
            S3Handler.upload(features,"emr/features");


        }
    }
}
