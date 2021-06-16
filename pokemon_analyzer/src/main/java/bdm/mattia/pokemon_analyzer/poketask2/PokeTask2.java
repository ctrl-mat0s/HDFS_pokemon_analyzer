package bdm.mattia.pokemon_analyzer.poketask2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import bdm.mattia.pokemon_analyzer.PokeTask;
import bdm.mattia.pokemon_analyzer.PokemonAnalyzerMain;

public class PokeTask2 extends PokeTask {
	
	public PokeTask2(String arg0, String arg1) {
		super(arg0, arg1);
	}

	@Override
	protected int start() throws Exception {
		
		//1st job corresponding to the 1st MapReduce cycle
		JobControl jobControl = new JobControl("ChainMapReduce");
		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1,"PokeTask2");
		job1.setJarByClass(PokemonAnalyzerMain.class);
		job1.setJobName("PokeTask2 1st_step");

		FileInputFormat.setInputPaths(job1, new Path(arg0));
		FileOutputFormat.setOutputPath(job1, new Path(arg1 + "/temp"));

		//Use of the CombinerClass passing the 1st ReducerClass
		job1.setMapperClass(PokeTask2Mapper1.class);
		job1.setReducerClass(PokeTask2Reducer1.class);
		job1.setCombinerClass(PokeTask2Reducer1.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);

		//2nd job corresponding to the 2nd MapReduce cycle
		jobControl.addJob(controlledJob1);
		Configuration conf2 = new Configuration();

		Job job2 = Job.getInstance(conf2);
		job2.setJarByClass(PokemonAnalyzerMain.class);
		job2.setJobName("PokeTask2 2nd_step");

		FileInputFormat.setInputPaths(job2, new Path(arg1 + "/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(arg1+"/final"));

		job2.setMapperClass(PokeTask2Mapper2.class);
		job2.setReducerClass(PokeTask2Reducer2.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);

		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);

		//Add the dependence between the two job
		controlledJob2.addDependingJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		Thread jobControlThread = new Thread(jobControl);
		jobControlThread.start();

		while (!jobControl.allFinished()) {
			System.out.println("Jobs in waiting state: "
					+ jobControl.getWaitingJobList().size());
			System.out.println("Jobs in ready state: "
					+ jobControl.getReadyJobsList().size());
			System.out.println("Jobs in running state: "
					+ jobControl.getRunningJobList().size());
			System.out.println("Jobs in success state: "
					+ jobControl.getSuccessfulJobList().size());
			System.out.println("Jobs in failed state: "
					+ jobControl.getFailedJobList().size());
			try {
				Thread.sleep(5000);
			} catch (Exception e) {
			}
		}
		return jobControl.getFailedJobList().size() == 0 ? 0:1;
	}
}
//public class PokeTask2 extends Configured implements Tool {
//
//	public int run(String[] args) throws Exception{
//
//		JobControl jobControl = new JobControl("Suicide 2 steps");
//		Configuration conf1 = getConf();
//
//		try {
//			Job job1 = Job.getInstance(conf1);
//			job1.setJarByClass(PokeTask2.class);
//			job1.setJobName("PokeTask2_1step");
//
//			FileInputFormat.setInputPaths(job1, new Path(args[0]));
//			FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
//
//			//Use of the CombinerClass passing the 1st ReducerClass
//			job1.setMapperClass(PokeTask2Mapper1.class);
//			job1.setReducerClass(PokeTask2Reducer1.class);
//			job1.setCombinerClass(PokeTask2Reducer1.class);
//
//			job1.setOutputKeyClass(Text.class);
//			job1.setOutputValueClass(IntWritable.class);
//
//
//			ControlledJob controlledJob1 = new ControlledJob(conf1);
//			controlledJob1.setJob(job1);
//
//			//2nd job corresponding to the 2nd MapReduce cycle
//			jobControl.addJob(controlledJob1);
//			Configuration conf2 = getConf();
//
//			Job job2 = Job.getInstance(conf2);
//			job2.setJarByClass(PokeTask2.class);
//			job2.setJobName("PokeTask2 2nd_step");
//
//			FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
//			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
//
//			job2.setMapperClass(PokeTask2Mapper2.class);
//			job2.setReducerClass(PokeTask2Reducer2.class);
//
//			job2.setMapOutputKeyClass(Text.class);
//			job2.setMapOutputValueClass(Text.class);
//
//			job2.setOutputKeyClass(Text.class);
//			job2.setOutputValueClass(Text.class);
//			job2.setInputFormatClass(KeyValueTextInputFormat.class);
//
//			ControlledJob controlledJob2 = new ControlledJob(conf2);
//			controlledJob2.setJob(job2);
//
//			//Add the dependence between the two job
//			controlledJob2.addDependingJob(controlledJob1);
//			jobControl.addJob(controlledJob2);
//			Thread jobControlThread = new Thread(jobControl);
//			jobControlThread.start();
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		while (!jobControl.allFinished()) {
//			System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
//			System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//			System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//			System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//			System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//			try {
//				Thread.sleep(5000);
//			} catch (Exception e) {
//			}
//
//		}
//		return jobControl.getFailedJobList().size() == 0 ? 0 : 1;
//	}
//
//	public int start(String[] args) throws Exception {
//		return ToolRunner.run(new PokeTask2(), args);
//
//	}
//}