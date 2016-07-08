package com.acxiom.pmp.mr.dataloadvalidation;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
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
				prop.getProperty(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON), 
				prop.getProperty(DWVALIDATION_SOURCE__EXCLUDED_TABLES));


		String dwTableInputDataSet = prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON) + FSEP + "Data" + FSEP +
		prop.getProperty(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
		//For Venkat Data
		//String dwTableInputDataSet =  prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON);
		Configuration conf= new Configuration();
		conf.set("mapreduce.job.reduces", "100");
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		conf.set(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE, prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE));
		conf.set(DWVALIDATION_COMPRESSION_LEVEL, prop.getProperty(DWConfigConstants.DWVALIDATION_COMPRESSION_LEVEL));
		conf.set(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON, prop.getProperty(DWConfigConstants.DWVALIDATION_SOURCE_TABLES_DATA_LOCATON));
		conf.set(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON, prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON));	
		String csTargetHeader = DWUtil.getTargetHeaderColumns(prop.getProperty(DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON) ,prop.getProperty(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE));
		conf.set(DWVALIDATION_TARGET_HEADER, csTargetHeader);
        String dateColIndexs = DWUtil.getDatecolIndx(csTargetHeader);
        conf.set(DATE_COL_INDEXS, dateColIndexs);
		Properties headerFileProps = DWUtil.getSourceHeaderFiles(sourceInputDataSet,prop.getProperty(DWConfigConstants.DWVALIDATION_COMPRESSION_LEVEL));
		String headerFilesStr = DWUtil.getSourceHeaderColumns(headerFileProps);
		conf.set(DWVALIDATION_SOURCE_HEADERS, headerFilesStr);		
		if(prop.getProperty(DWConfigConstants.DWVALIDATION_COMPRESSION_LEVEL)==COMPRESSION_FULL){
			String srcTablesCompList = DWUtil.getFullSrcTablesList(headerFileProps);
			conf.set(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE, srcTablesCompList);
		}else{
			conf.set(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE, prop.getProperty(DWConfigConstants.DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE));
		}
		conf.set(DWVALIDATION_ROW_KEY,prop.getProperty(DWVALIDATION_ROW_KEY));
		String outPath = prop.getProperty(DWVALIDATION_RESULT_LOCATION ) + prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE) + FSEP + 
				prop.getProperty(DWVALIDATION_START_DATAE) + UNDERSCORE + prop.getProperty(DWVALIDATION_END_DATAE);
		conf.set(DWVALIDATION_RESULT_LOCATION, outPath);
		conf.set(DWVALIDATION_COL_SAMPLING_COUNT, prop.getProperty(DWConfigConstants.DWVALIDATION_COL_SAMPLING_COUNT));
		
		
		//conf.set("mapreduce.job.reduces", "7");
		
		/////temp code need to delete
		/*Map<String, Map<String, String>> map1 = DWUtil.getHeadersAsMap(headerFilesStr);
	    Map<String, Map<String, String>> dateColIndxMap = new HashMap<String, Map<String, String>>();
	   
	    dateColIndxMap = DWUtil.getDatecolIndx(map1);
		for(Map.Entry<String, Map<String, String>> elements:dateColIndxMap.entrySet() ){

			 System.out.println(elements.getKey());
			  Map<String, String> map2 = elements.getValue();
			  for(Map.Entry<String, String>datecoulmns:map2.entrySet() ){
				  System.out.println("Date:" + datecoulmns.getKey());
				  System.out.println("ColumnIndex:" + datecoulmns.getValue());
			  }
		}*/
		// 
		Job job = new Job(conf, "DWValidation");
		job.setJarByClass(ValidatorRunner.class);
		job.setMapperClass(ValidatorMapper.class);
		job.setReducerClass(ValidatorReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);			
		job.setOutputKeyClass(Text.class);	
		job.setOutputValueClass(NullWritable.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		//job.setNumReduceTasks(0);
		// inputs
		job.setInputFormatClass(TextInputFormat.class);
		String inputDataSet = sourceInputDataSet + COMMA + dwTableInputDataSet;
		//System.out.println("FullInputData: " +inputDataSet);
		//log.info("Input Path:"+inputDataSet);
		FileInputFormat.setInputPaths(job, inputDataSet);

		// output
		//job.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path(prop.getProperty(DWVALIDATION_RESULT_LOCATION));
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