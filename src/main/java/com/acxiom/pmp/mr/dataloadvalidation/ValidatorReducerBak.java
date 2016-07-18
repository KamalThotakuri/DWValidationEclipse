package com.acxiom.pmp.mr.dataloadvalidation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWException;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;

public class ValidatorReducerBak extends Reducer< Text, Text, Text, NullWritable> implements DWConfigConstants {
	private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
	private Map<String, Boolean> targetHiveTableFlagMap = new HashMap<String, Boolean>();
	private static Logger log = LoggerFactory.getLogger(ValidatorReducerBak.class);
	private static Map<String, Integer> diffReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> sourceReportingColLimit = new HashMap<String, Integer>();
	private static Map<String, Integer> targetReportingColLimit = new HashMap<String, Integer>();
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable;
	private String csTargetHeader;
	private String srcRequiredTable;
	private MultipleOutputs<Text, NullWritable> out;
	private String rootOutputLoc;
	private int limitCount;

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		String headerFiles = conf.get(DWVALIDATION_SOURCE_HEADERS);
		targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
		csTargetHeader = conf.get(DWVALIDATION_TARGET_HEADER);
		srcRequiredTable = conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE);
		rootOutputLoc = conf.get(DWVALIDATION_RESULT_LOCATION);
		String limit = conf.get(DWVALIDATION_COL_SAMPLING_COUNT);
		limitCount = Integer.parseInt(limit);
		map = DWUtil.getHeadersAsMap(headerFiles);
		out = new MultipleOutputs(context);		

	}


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException{

		Map<String, String> targetHiveTableMap = new HashMap<String, String>();
		Map<String, String> soruceTableMap = new HashMap<String, String>();

/*	   variables that need to be removed	
      //String tableName=null;
		String matchedColsHolder=null;
		String differedColssHolder=null;
		String existsOnlyinSource=null;
		String existsOnlyinTarget=null;
		//String notUpdateDtoNullCols=null;
		String upDatedtoNullCols=null;
		String result=null;
		StringBuilder onlySourceCols = new StringBuilder();
		StringBuilder onlyTargetCols = new StringBuilder();
		StringBuilder matchedCols = new StringBuilder();
		StringBuilder differedCols = new StringBuilder();		
		StringBuilder notUpdatetoNull = new StringBuilder();
		StringBuilder updatetoNull = new StringBuilder();
		//String date = "";
*/
		log.info("reducer messaged from log4j");
		int itrDepth=0;
		boolean targetTableExist=false;
		int targetCount =0;
		for(Text tableDateData : values){

			String[] tableDateDataArr = tableDateData.toString().split(APPENDER,5);
			String tableName = tableDateDataArr[0];
			String date = tableDateDataArr[1];
			String inFileName = tableDateDataArr[2];
			String mapperSplitValue = tableDateDataArr[3];
			String record = tableDateDataArr[4];

			//tempDelete
			StringBuilder ss = new StringBuilder();
			if(!tableName.equals(targetHiveTable)){
				Map<String, String> dateHeader = map.get(tableName);
				String header = dateHeader.get(date);
				String[] cols = header.split(COMMA);
				String[] colValues = record.split(TAB,-2);
				try{
				/*	if(cols.length != colValues.length ){
						throw new Exception();
					}*/
					for(int i = 0; i < cols.length; i++) {
						String sCName = cols[i].toUpperCase();
						if(sCName.contains(UPDT_DT)){
							sCName = sCName.replace(UPDT_DT, UPD_DT);
						}else if(sCName.toUpperCase().contains(UPDATE_DATE)){
							sCName = sCName.replace(UPDATE_DATE, UPD_DT);
						}else if(cols[i].toUpperCase().contains(UPT_DT)){
							sCName = sCName.replace(UPT_DT, UPD_DT);
						}
						String scValue = colValues[i];
						scValue = scValue.replace(QUOTES, EMPTY);
						soruceTableMap.put(sCName, scValue + TILD + inFileName );	
						/*ss.append(colValues[i]);
						ss.append(";")*/;
					}
				}catch(Exception e){
					//e.printStackTrace();
					String exTrace = DWUtil.getStackTraceAsString(e);
					log.error("Error occured while validating the source col values with its header"+e.getMessage());
					log.error(exTrace);
					throw new DWException("Count Didn't Match " + "ccount:" + cols.length + "cVcount:" + colValues.length + "TableName: " +tableName + " HeaderCOunt:"+ cols.length + " RecordCunt:" +colValues.length + 
							//" HeaderCols:" +header +
							" MapperSplitValue:" + mapperSplitValue +
							" InputFileName:" + inFileName +
							" Record:" + record +
							" SplittedRecord:"+ss.toString(), e);
				}
			}else {
				/// Loading of Target HashMap

				targetTableExist=true;
				String[] cols = csTargetHeader.split(COMMA);
				String[] colValues = record.split(THORN,-2);
				try{
					/*if(cols.length != colValues.length ){
						throw new Exception();
					}*/
					for(int i = 0; i < cols.length; i++) {
						String tcName = cols[i].toUpperCase();
						if(tcName.contains(UPDT_DT)){
							tcName = tcName.replace(UPDT_DT, UPD_DT);
						}else if(tcName.toUpperCase().contains(UPDATE_DATE)){
							tcName = tcName.replace(UPDATE_DATE, UPD_DT);
						}else if(cols[i].toUpperCase().contains(UPT_DT)){
							tcName = tcName.replace(UPT_DT, UPD_DT);
						}
						String tcValue = colValues[i];
						tcValue = tcValue.replace(QUOTES, EMPTY);
						/*if((tcValue.startsWith(QUOTES)) && tcValue.endsWith(QUOTES)){
							tcValue = tcValue.replace(QUOTES, EMPTY);
						}*/
						targetHiveTableMap.put(tcName, tcValue + TILD + inFileName );
						targetHiveTableFlagMap.put(tcName, false);
					}
				}catch(Exception e){
					//e.printStackTrace();
					String exTrace = DWUtil.getStackTraceAsString(e);
					log.error("Error occured while validating the target col values with its header "+e.getMessage());
					log.error(exTrace);
					throw new DWException("Count Didn't Match " + "ccount:" + cols.length + "cVcount:" + colValues.length + "TableName: " + tableName + " HeaderCOunt:"+ cols.length + " RecordCunt:" +colValues.length + 
							" Record:" + record +							
							" MapperSplitValue:" + mapperSplitValue +
							" InputFileName:" + inFileName, e);
				}
				targetCount ++;
			}
			itrDepth ++;
		}

		try{
			if(itrDepth >1 && targetTableExist){
				for (String colName : soruceTableMap.keySet()) {
					String targetColValueFileName = targetHiveTableMap.get(colName);
					String sourceColValueFilName = soruceTableMap.get(colName);
					String[] sourceValues = sourceColValueFilName.split(TILD);
					String sourceColVal = sourceValues[0];
					String sourceFileName = sourceValues[1];
					if(targetColValueFileName !=null ){
						String[] targetValues = targetColValueFileName.split(TILD);
						String targeColVal = targetValues[0];
						String targetFileName = targetValues[1];
						if (sourceColVal.isEmpty() && targeColVal.equals(ORC_NULL)){
							//matchedCols.append(colName);
							//matchedCols.append(SEMICOLON);
							targetHiveTableFlagMap.put(colName, true);
						}else{
							if(!sourceColVal.equals(targeColVal)){
								String sKvalue =key.toString();
								String sDiff = appendDifferedResult(colName, sourceColVal, targeColVal, sKvalue, sourceFileName,  targetFileName);
								keyOut.set(sDiff);
								String resultLocation = rootOutputLoc + FSEP + DIFFERED_COLS;
								out.write(keyOut, NullWritable.get(), resultLocation);							
								targetHiveTableFlagMap.put(colName, true);
								int currentValue =0;
								String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  DIFFERED_COLS + UNDERSCORE +DWVALIDATION_COL_SAMPLING_COUNT ;
								if(diffReportingColLimit.get(colName) == null){
									diffReportingColLimit.put(colName, 0);													
									keyOut.set(sDiff);									
									out.write(keyOut, NullWritable.get(), smplingResultLocation);	
								}else{
									currentValue = diffReportingColLimit.get(colName);
									if(currentValue<limitCount){
										keyOut.set(sDiff);									
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
										diffReportingColLimit.put(colName, ++currentValue);
										int temp = diffReportingColLimit.get(colName);
										if(temp>10){
											throw new DWException("Looping more time" + Integer.toString( temp));
										}
									}
								}
							}else{
								//matchedCols.append(colName);
								//matchedCols.append(SEMICOLON);
								targetHiveTableFlagMap.put(colName, true);
							}
						}
					}else if(targetHiveTableMap.get(colName) ==null ){
						String soruceDiff = appendSourceResult(colName, sourceFileName);
						keyOut.set(soruceDiff);
						String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS ;
						out.write(keyOut, NullWritable.get(), resultLocation);	
						///Printing limit output
						int currentValue =0;
						String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  EXISTS_ONLY_IN_SOURCE_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
						if(sourceReportingColLimit.get(colName) == null){
							sourceReportingColLimit.put(colName, 0);
							out.write(keyOut, NullWritable.get(), smplingResultLocation);	
						}else{
							currentValue = sourceReportingColLimit.get(colName);
							if(currentValue<limitCount){
								out.write(keyOut, NullWritable.get(), smplingResultLocation);	
								sourceReportingColLimit.put(colName, currentValue +1);
								int temp = sourceReportingColLimit.get(colName);
								if(temp>limitCount){
									throw new DWException("Looping more time" + Integer.toString( temp));
								}
							}
						}

					}
				}
				for(String colName:targetHiveTableFlagMap.keySet()){
					String[] colTableNameHolder = colName.split(UNDERSCORE);
					String colTableName = colTableNameHolder[0];
					if(srcRequiredTable.contains(colTableName)){
						if(!(targetHiveTableFlagMap.get(colName))){
							String[] targetValues = targetHiveTableMap.get(colName).split(TILD);
							String targeColVal = targetValues[0];
							String targetFileName = targetValues[1];
							/*if(targeColVal=="null"){
								updatetoNull.append(colName);
								updatetoNull.append(TAB);
								updatetoNull.append(key.toString());								
							}else{*/
							String targetDiff = appendSourceResult(colName, targetFileName);
							keyOut.set(targetDiff);							
							String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_TARGET_COLS;
							out.write(keyOut, NullWritable.get(), resultLocation);								
							//prinitng limit output
							int currentValue =0;
							String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  EXISTS_ONLY_IN_TARGET_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
							if(targetReportingColLimit.get(colName) == null){
								targetReportingColLimit.put(colName, 0);
								out.write(keyOut, NullWritable.get(), smplingResultLocation);	
							}else{
								currentValue = targetReportingColLimit.get(colName);
								if(currentValue<limitCount){
									out.write(keyOut, NullWritable.get(), smplingResultLocation);	
									targetReportingColLimit.put(colName, ++currentValue);
									int temp = targetReportingColLimit.get(colName);
									if(temp>10){
										throw new DWException("Looping more time" + Integer.toString( temp));
									}
								}
							}

						}
					}
				}

			}else if (itrDepth >=1 && !targetTableExist){
				for(String colName : soruceTableMap.keySet()) {
					String sourceColValueFilName = soruceTableMap.get(colName);
					String[] sourceValues = sourceColValueFilName.split(TILD);
					String sourceColVal = sourceValues[0];
					String sourceFileName = sourceValues[1];
					String soruceDiff = appendSourceResult(colName, sourceFileName);
					keyOut.set(soruceDiff);
					String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS ;
					out.write(keyOut, NullWritable.get(), resultLocation);	
				}
			}else if (itrDepth==1 && targetTableExist){
				//result = key.toString() + COLON + "<Not in comparision Date range>" +PIPE + " ReduceSideFile:" + fileNameHolders.toString(); 
				//context.write(new Text(result), NullWritable.get());
				//result = key.toString() + "::" + record ;
				//result = key.toString()  ;
				//context.write(new Text(result), NullWritable.get());
			}
		}catch (IOException | InterruptedException e) {
			//e.printStackTrace();
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while reading targetheaderfile "+e.getMessage());
			log.error(exTrace);
		}
	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		//System.out.println("Required Tables:" + srcRequiredTable);
		out.close();

	}

	private static String appendDifferedResult(String colName, String sCval, String tCval, String key, String sFile, String tFile){
		StringBuilder sb = new StringBuilder();
		String COMMA = ",";
		sb.append(colName);
		sb.append(COMMA);
		sb.append(sCval);
		sb.append(COMMA);
		sb.append(tCval);
		sb.append(COMMA);
		sb.append(key.toString());
		sb.append(COMMA);
		sb.append(sFile);
		sb.append(COMMA);
		sb.append(tFile);
		return sb.toString();
	}

	private static String appendSourceResult(String colName, String sFile){
		StringBuilder sb = new StringBuilder();
		String COMMA = ",";
		sb.append(colName);
		sb.append(COMMA);
		sb.append(sFile);
		return sb.toString();
	}


	private static String appendTargetResult(String colName, String tFile){
		StringBuilder sb = new StringBuilder();
		String COMMA = ",";
		sb.append(colName);
		sb.append(COMMA);
		sb.append(tFile);
		return sb.toString();

	}
}
