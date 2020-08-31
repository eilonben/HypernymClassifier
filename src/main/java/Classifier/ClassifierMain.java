package Classifier;


import utils.S3Handler;
import weka.classifiers.Evaluation;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.trees.J48;
import weka.core.converters.ConverterUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.*;

public class ClassifierMain {

    private static final String FOLDER_INPUT = "classifier_files";
    private static final String FOLDER_OUTPUT = "classifier_output";
    private static final String FILE_1 = "proc_corpus_vectors.arff";
    private static final String FILE_2 = "proc_corpus_wordsAndVectors.arff";

    public static void main(String[] args) throws Exception {

        java.nio.file.Path path = Paths.get(FOLDER_INPUT);
        createFolderIfNotExists(path);

        path = Paths.get("output");
        createFolderIfNotExists(path);

        S3Handler s3Handler =new S3Handler();
        BufferedReader brOutputJob2 = s3Handler.download("output2/part-r-00000");
        BufferedReader brNumberOfFeatures = s3Handler.download("emr/features.txt");

        BufferedWriter bw_corpus_vectors = new BufferedWriter(new FileWriter(new File(FOLDER_INPUT +"/proc_corpus_vectors.arff")));
        BufferedWriter bw_corpus_wordsAndVectors = new BufferedWriter(new FileWriter(new File(FOLDER_INPUT +"/proc_corpus_wordsAndVectors.arff")));

        int lengthVectorFeatures = Integer.parseInt(brNumberOfFeatures.readLine());
        brNumberOfFeatures.close();
        System.out.println("# features : " + lengthVectorFeatures);

        bw_corpus_vectors.write("@RELATION nounpair\n\n");
        bw_corpus_wordsAndVectors.write("@RELATION nounpair\n\n@ATTRIBUTE nounPair STRING\n");

        writeFeaturesIndexesToFile(bw_corpus_vectors, lengthVectorFeatures);      // A number of features to indexes
        writeFeaturesIndexesToFile(bw_corpus_wordsAndVectors, lengthVectorFeatures);

        bw_corpus_vectors.write("@ATTRIBUTE ans {true, false}\n\n@DATA\n");
        bw_corpus_wordsAndVectors.write("@ATTRIBUTE ans {true, false}\n\n@DATA\n");

        BufferedWriter bwOutputJob2 = new BufferedWriter(new FileWriter("output/part-r-00000"));       // file for copy job2 output, need for classifier
        // read the lines from job2 output to arff files
        readJob2OutputToFiles(brOutputJob2, bw_corpus_vectors, bw_corpus_wordsAndVectors, bwOutputJob2);
        brOutputJob2.close();
        bw_corpus_vectors.close();
        bw_corpus_wordsAndVectors.close();
        bwOutputJob2.close();

        path = Paths.get(FOLDER_INPUT);
        createFolderIfNotExists(path);

        path = Paths.get(FOLDER_OUTPUT);
        createFolderIfNotExists(path);

        // cross-validate
        weka.classifiers.Classifier cla = new J48();
        Instances data = DataSource.read(FOLDER_INPUT + "/"+FILE_1);
        data.setClassIndex(data.numAttributes() - 1);

        getStatistics(cla, data); // print statistics

        // train and test
        cla.buildClassifier(data);
        Instances testData = DataSource.read(FOLDER_INPUT + "/"+FILE_1); // get data from path
        testData.setClassIndex(data.numAttributes() - 1);
        Instances claData = new Instances(testData);

        setClassValuesForAllInstance(cla, testData, claData);

        ConverterUtils.DataSink.write(FOLDER_OUTPUT + "/classifiedSet.arff", claData);
        claData = DataSource.read(FOLDER_OUTPUT + "/classifiedSet.arff");


        //tp fp tn fn examples
        int numOfExamples = 100;
        List<String> tpExamples =new ArrayList<String>();
        List<String> fpExamples =new ArrayList<String>();
        List<String> tnExamples =new ArrayList<String>();
        List<String> fnExamples =new ArrayList<String>();

        BufferedReader brWordsAndVectors = new BufferedReader(new FileReader(FOLDER_INPUT + "/"+FILE_2));

        while (!brWordsAndVectors.readLine().contains("@DATA")) {}// skip to data section

        String line = brWordsAndVectors.readLine();
        for (int i = 0; i < data.size(); i++, line = brWordsAndVectors.readLine()) {

            boolean valueTrain = getValueTrain(data, i);
            boolean valueTest = getValueTrain(claData, i);

            if (valueTrain && valueTest &&  tpExamples.size() < numOfExamples)
                tpExamples.add(line);
            else if (!valueTrain && valueTest &&  fpExamples.size() < numOfExamples)
                fpExamples.add(line);
            else if (!valueTrain && !valueTest && tnExamples.size() < numOfExamples)
                tnExamples.add(line);
            else if (valueTrain && !valueTest && fnExamples.size() < numOfExamples)
                fnExamples.add(line);
        }

        printExamples(tpExamples, fpExamples, tnExamples, fnExamples);

    }

    private static void readJob2OutputToFiles(BufferedReader brOutputJob2, BufferedWriter bw1, BufferedWriter bw2, BufferedWriter bwOutputJob2) throws IOException {
        String outputFromJob2Line;
        while (!((outputFromJob2Line = brOutputJob2.readLine()) == null)) {
            bw2.write(outputFromJob2Line + "\n");                                         // with the word pair -> w1$w2 vector
            bw1.write(outputFromJob2Line.substring(outputFromJob2Line.indexOf("\t") + 1) + "\n");       // without the word pair -> vector
            bwOutputJob2.write(outputFromJob2Line);
        }
    }

    private static void writeFeaturesIndexesToFile(BufferedWriter bw2, int lengthVectorFeatures) throws IOException {
        for (int i = 0; i < lengthVectorFeatures; i++) {
            bw2.write("@ATTRIBUTE p" + i + " REAL\n");
        }
    }

    private static void printExamples(List<String> tpExamples, List<String> fpExamples, List<String> tnExamples, List<String> fnExamples) {
        System.out.println("\n\ntp Examples:");
        for (String s : tpExamples)
            System.out.println(s.replace("\t","\n"));
        System.out.println("\n\nfp Examples:");
        for (String s : fpExamples)
            System.out.println(s.replace("\t","\n"));
        System.out.println("\n\ntn Examples:");
        for (String s : tnExamples)
            System.out.println(s.replace("\t","\n"));
        System.out.println("\n\nfn Examples:");
        for (String s : fnExamples)
            System.out.println(s.replace("\t","\n"));
    }

    private static boolean getValueTrain(Instances data, int i) {
        String trainSetEntry = data.get(i).toString();
        return trainSetEntry.substring(trainSetEntry.lastIndexOf(",") + 1).equals("true");
    }

    private static void setClassValuesForAllInstance(weka.classifiers.Classifier cla, Instances testData, Instances claData) throws Exception {
        for (int i = 0; i < testData.numInstances(); i++)
            claData.instance(i).setClassValue(cla.classifyInstance(testData.instance(i)));
    }

    private static void getStatistics(weka.classifiers.Classifier cla, Instances data) throws Exception {
        int folds = 10;
        Evaluation eval_crossValidation = new Evaluation(data);
        eval_crossValidation.crossValidateModel(cla, data, folds, new Random(1));
        //Statistics
        System.out.println(eval_crossValidation.toSummaryString("\nCross validation - Statistics \n\n", false));
        //Detailed class statistics
        System.out.println(eval_crossValidation.toClassDetailsString("\nCross validation - Detailed class statistics \n\n"));
    }

    private static void createFolderIfNotExists(Path path) throws IOException {
        if (!Files.exists(path))
            Files.createDirectory(path);
    }

}
