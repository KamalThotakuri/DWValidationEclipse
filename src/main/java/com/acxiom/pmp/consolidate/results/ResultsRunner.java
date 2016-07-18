package com.acxiom.pmp.consolidate.results;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.helpers.OnlyOnceErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWConfiguration;
import com.acxiom.pmp.constants.DWConfigConstants;
import com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner;

public class ResultsRunner extends Configured implements Tool,DWConfigConstants {
	private static Logger log = LoggerFactory.getLogger(ValidatorRunner.class);
	private String confIdentifier;
	FileSystem fs;

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		 String confIdentifier = args[0];
		switch (confIdentifier.toUpperCase()) {		
		case "BIN":
			confIdentifier = BIN;
			break;
		case "PIN":
			confIdentifier = PIN;
			break;
		case "PBL": 
			confIdentifier = PBL;
			break;
		case "ADDRESS":
			confIdentifier = ADDRESS;
			break;
		case "ZIP":
			confIdentifier = ZIP9;
			break;			
		default:
			log.error("Not given the proper configuration file name");
			System.exit(-1);
			break;
		}
		String path = "config" + FSEP + confIdentifier;
		System.out.println("config Location========" + path);
		DWConfiguration.loadProps(path);
		Properties prop =  DWConfiguration.getProps();		
		String resultMainFolder = prop.getProperty(DWVALIDATION_RESULT_LOCATION ) + prop.getProperty(DWConfigConstants.DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE) + FSEP + 
				prop.getProperty(DWVALIDATION_START_DATAE) + UNDERSCORE + prop.getProperty(DWVALIDATION_END_DATAE);
		String resultFolder = resultMainFolder + FSEP + SAMPLING_FOLDER_NAME + FSEP;
		String mergedDiffResultFile = resultFolder + MERGED_DIFFERED_COLS; 
		String mergedExistTargetResultFile = resultFolder + MERGED_EXISTS_ONLY_IN_TARGET_COLS;
		String mergedExistSrcResultFile = resultFolder + MERGED_EXISTS_ONLY_IN_SOURCE_COLS;
				
		Path inFile = new Path(resultFolder);
		fs = FileSystem.get(new Configuration());
		FileStatus[] fileStatus = fs.listStatus(inFile);
		for(FileStatus status : fileStatus){
			if(!status.isDir()){
				continue ;				
			}
			String resultFileName = status.getPath().getName().toString();
			String resultLimitfile = status.getPath().toString();
			if(resultFileName.contains(DIFFERED_COLS)){
				mergeDifferedcols(resultLimitfile, mergedDiffResultFile );
			}else if(resultFileName.contains(EXISTS_ONLY_IN_SOURCE_COLS)){
				mergeDifferedcols(resultLimitfile, mergedExistSrcResultFile );
			}else if(resultFileName.contains(EXISTS_ONLY_IN_TARGET_COLS)){
				mergeDifferedcols(resultLimitfile, mergedExistTargetResultFile );
			}
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		ResultsRunner vr = new ResultsRunner();
		int res = ToolRunner.run(conf, vr, args);
		System.exit(res);       

	}
	
	private static void mergeDifferedcols(String strinpuptFileName, String outputFileName ) throws IOException{
		
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream is = fs.open(new Path(strinpuptFileName));
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		//tsHeaderContent = br.readLine();
		
	}
	
    private static void mergeTartetexistcols(String strinpuptFileName, String outputFileName ){
		
	}
    
    private static void mergeSourceexistcols(String strinpuptFileName, String outputFileName ){
		
	}
	
}
