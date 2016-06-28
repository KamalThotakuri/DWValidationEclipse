package com.acxiom.pmp.mr.dataloadvalidation;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWConfiguration;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;
public class ValidatorRunner extends Configured implements Tool,DWConfigConstants{
	private static Logger log = LoggerFactory.getLogger(ValidatorRunner.class);

	@Override
	public int run(String[] args) throws Exception {
		DWConfiguration.loadProps("config/dwconfig.properties");
		Properties prop =  DWConfiguration.getProps();
		String sourceInputDataSet = DWUtil.parseSoruceTableInput(prop.getProperty(DWVALIDATION_START_DATAE), 
				prop.getProperty(DWVALIDATION_END_DATAE), 
				prop.getProperty(DWVALIDATION_COMPRESSION_LEVEL),
				prop.getProperty(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE),
				prop.getProperty(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE),
				prop.getProperty(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON));

		System.out.println(sourceInputDataSet);
		//String dwTableInputDataSet = prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON) + FSEP + "Data" + FSEP +
		prop.getProperty(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
		//For Venkat Data
		String dwTableInputDataSet = "/mapr/thor/amexprod/STAGING/1dataload/1TIME/Data/20160617/PBL/joinFiles/";
		Configuration conf= new Configuration();
		conf.set("mapred.reduce.tasks", "7");
		conf.set(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE, prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE));
		conf.set(DWVALIDATION_COMPRESSION_LEVEL, prop.getProperty(DWConfigConstants.DWVALIDATION_COMPRESSION_LEVEL));
		conf.set(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON, prop.getProperty(DWConfigConstants.DWVALIDATION_SOURCE_TABLES_DATA_LOCATON));
		conf.set(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON, prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON));	
		String csTargetHeader = DWUtil.getTargetHeaderColumns(prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON) ,prop.getProperty(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE));
		conf.set(DWVALIDATION_TARGET_HEADER, csTargetHeader);
		Properties headerFileProps = DWUtil.getSourceHeaderFiles(sourceInputDataSet);
		String headerFilesStr = DWUtil.getSourceHeaderColumns(headerFileProps);
		conf.set(DWVALIDATION_SOURCE_HEADERS, headerFilesStr);
		if(prop.getProperty(DWConfigConstants.DWVALIDATION_COMPRESSION_LEVEL)==COMPARISION_TYPE){
			String srcTablesCompList = DWUtil.getFullSrcTablesList(headerFileProps);
			conf.set(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE, srcTablesCompList);
		}else{
			conf.set(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE, prop.getProperty(DWConfigConstants.DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE));
		}
		conf.set(DWVALIDATION_ROW_KEY,prop.getProperty(DWVALIDATION_ROW_KEY));
		conf.set("mapreduce.job.reduces", "7");
		Map<String, Map<String, String>> map1 = DWUtil.getHeadersAsMap(headerFilesStr);
		for(Map.Entry<String, Map<String, String>> elements:map1.entrySet() ){
			
			 System.out.println(elements.getKey());
			  Map<String, String> map2 = elements.getValue();
			  for(Map.Entry<String, String>datecoulmns:map2.entrySet() ){
				  System.out.println("Date:" + datecoulmns.getKey());
				  System.out.println("Columns:" + datecoulmns.getValue());
			  }
		}
		// 
		Job job = new Job(conf, "DWValidation");
		job.setJarByClass(ValidatorRunner.class);
		job.setMapperClass(ValidatorMapper.class);
		job.setReducerClass(ValidatorReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);			
		job.setOutputKeyClass(Text.class);	
		job.setOutputValueClass(NullWritable.class);
		//job.setNumReduceTasks(0);
		// inputs
		job.setInputFormatClass(TextInputFormat.class);
		String inputDataSet = sourceInputDataSet + COMMA + dwTableInputDataSet;
		System.out.println("FullInputData: " +inputDataSet);
		log.info("Input Path:"+inputDataSet);
		FileInputFormat.setInputPaths(job, inputDataSet);

		// output
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path(prop.getProperty(DWVALIDATION_RESULT_LOCATION));
		/*String outPath = prop.getProperty(DWVALIDATION_RESULT_LOCATION ) + prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE) + FSEP + 
				prop.getProperty(DWVALIDATION_START_DATAE) + UNDERSCORE +
				prop.getProperty(DWVALIDATION_END_DATAE); 
		Path outputPath = new Path(outPath);*/
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			outputPath.getFileSystem(conf).delete(outputPath,true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		log.info("Submitting the job");
		boolean b = job.waitForCompletion(true);
		System.out.println("b is "+b);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ValidatorRunner vr = new ValidatorRunner();
		int res = ToolRunner.run(conf, vr, args);
		System.exit(res);       
	}
}