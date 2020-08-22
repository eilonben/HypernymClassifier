package EMR;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.S3Handler;
import utils.Stemmer;

import java.io.*;
import java.util.HashMap;
import java.util.Scanner;

public class Step2 {
    public static class MapperStep2 extends Mapper<LongWritable, Text, Text, LongWritable> {
        private File depPaths2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            S3Handler s3 = new S3Handler();
            BufferedReader brPaths = s3.download("emr/depPaths.txt");
            depPaths2 = new File("depPaths2.txt");
            depPaths2.createNewFile();
            BufferedWriter bwPaths = new BufferedWriter(new FileWriter(depPaths2));
            String line;
            while ((line = brPaths.readLine()) != null) {
                bwPaths.write(line + "\n");
            }
            bwPaths.close();
            brPaths.close();
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
            long index = 0;
            boolean found = false;
            String[] split = line.toString().split("\\s");
            Scanner scanner = new Scanner(depPaths2);
            while (scanner.hasNextLine()) {
                if (split[1].equals(scanner.nextLine())) {
                    found = true;
                    break;
                } else
                    index++;
            }
            if (found) {
                context.write(new Text(split[0]), new LongWritable(index));
            }
            scanner.close();
        }
    }

    public static class ReducerStep2 extends Reducer<Text, LongWritable, Text, Text> {
        private HashMap<Text, Boolean> annotatedSet;
        private int featuresCount;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            Stemmer stemmer = new Stemmer();
            S3Handler s3 = new S3Handler();

            BufferedReader brHyper = s3.download("emr/hypernym.txt");
            BufferedReader brCount = s3.download("emr/features.txt");
            String featuresNum = brCount.readLine();
            featuresCount = Integer.parseInt(featuresNum);
            brCount.close();
            annotatedSet = new HashMap<>();
            String line = null;
            while ((line = brHyper.readLine()) != null) {
                String[] split = line.split("\\s");
                stemmer.add(split[0].toCharArray(), split[0].length());
                stemmer.stem();
                String word1 = stemmer.toString();
                stemmer.add(split[1].toCharArray(), split[1].length());
                stemmer.stem();
                String word2 = stemmer.toString();
                annotatedSet.put(new Text(word1 + "#" + word2), split[2].equals("True"));
            }
            brHyper.close();

        }

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            if (annotatedSet.containsKey(key)) {
                long[] vector = new long[featuresCount];
                for (LongWritable index : values) {
                    vector[(int) index.get()] += 1;
                }
                StringBuilder sb = new StringBuilder();
                for (long index : vector)
                    sb.append(index).append(",");
                sb.append(annotatedSet.get(key));
                context.write(key, new Text(sb.toString()));
            }
        }
    }
}
