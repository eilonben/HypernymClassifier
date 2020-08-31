package EMR;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.log4j.BasicConfigurator;

public class HypernymApplication {
    private static final String DP_MIN = "3";

    public static void main(String[] args) {

        BasicConfigurator.configure();
        final AmazonElasticMapReduce emr = AmazonElasticMapReduceClient.builder()
                .withRegion(Regions.US_EAST_1)
                .build();

        HadoopJarStepConfig jarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://appbucket305336118/jarbacket/HypernymClassification.jar")
                .withMainClass("EMR.MainPipeline")
                .withArgs("s3n://inputfiledsp2020/biarcs.00-of-99", "s3n://bucket1586960757979w/intermediate2/","s3n://bucket1586960757979w/output2/", DP_MIN);

        StepConfig step1Config = new StepConfig()
                .withName("Main Pipeline")
                .withHadoopJarStep(jarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.7.2").withEc2KeyName("elion")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("hypernyms")
                .withInstances(instances)
                .withSteps(step1Config)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withLogUri("s3n://appbucket305336118/logs/")
                .withReleaseLabel("emr-5.0.0")
                .withBootstrapActions();

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        }



}
