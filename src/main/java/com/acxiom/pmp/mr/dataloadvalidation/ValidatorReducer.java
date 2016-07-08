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

public class ValidatorReducer extends Reducer< Text, Text, Text, NullWritable> implements DWConfigConstants {
	private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
	private Map<String, Boolean> targetHiveTableFlagMap = new HashMap<String, Boolean>();
	private static Logger log = LoggerFactory.getLogger(ValidatorReducer.class);
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



		log.info("reducer messaged from log4j");
		int itrDepth=0;
		boolean targetTableExist=false;
		int targetCount =0;
		for(Text tableDateData : values){

			String[] tableDateDataArr = tableDateData.toString().split(COLON,5);
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
					for(int i = 0; i < cols.length; i++) {
						soruceTableMap.put(cols[i], colValues[i] + TILD + inFileName );	
						/*ss.append(colValues[i]);
						ss.append(";")*/;
					}
				}catch(Exception e){
					//e.printStackTrace();
					throw new DWException("Count Didn't Match TableName: " + tableName + " HeaderCOunt:"+ cols.length + " RecordCunt:" +colValues.length + 
							//" HeaderCols:" +header +
							" MapperSplitValue:" + mapperSplitValue +
							" InputFileName:" + inFileName +
							" Record:" + record +
							" SplittedRecord:"+ss.toString(), e);
					//throw new DWException("Kamal", e);
				}
			}else {
				/// Loading of Target HashMap
				if(targetCount == 1){
					throw new DWException(record );
				}
				targetTableExist=true;
				String[] cols = csTargetHeader.split(COMMA);
				String[] colValues = record.split(TAB,-2);
				try{
					for(int i = 0; i < cols.length; i++) {
						targetHiveTableMap.put(cols[i], colValues[i] + TILD + inFileName );
						targetHiveTableFlagMap.put(cols[i], false);
					}
				}catch(Exception e){
					//e.printStackTrace();
					throw new DWException("Count Didn't Match TableName: " + tableName + " HeaderCOunt:"+ cols.length + " RecordCunt:" +colValues.length + 
							" Record:" + record +							
							" MapperSplitValue:" + mapperSplitValue +
							" InputFileName:" + inFileName, e);
					//throw new DWException("Kamalkumar", e);
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
						if(!sourceColVal.equals(targeColVal)){
							if (sourceColVal.isEmpty() && targeColVal == "null"){
								matchedCols.append(colName);
								matchedCols.append(SEMICOLON);
								targetHiveTableFlagMap.put(colName, true);
							}else{
								differedCols.append(colName);
								differedCols.append(TAB);
								differedCols.append(sourceColVal);
								differedCols.append(TAB);
								differedCols.append(targeColVal);
								differedCols.append(TAB);
								differedCols.append(key.toString());
								differedCols.append(TAB);
								differedCols.append(sourceFileName);
								/*differedCols.append(TAB);
								differedCols.append(targetFileName);*/								
								keyOut.set(differedCols.toString());
								String resultLocation = rootOutputLoc + FSEP + DIFFERED_COLS;
								out.write(keyOut, NullWritable.get(), resultLocation);							
								targetHiveTableFlagMap.put(colName, true);
								differedCols.setLength(0);	
								int currentValue =0;
								String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  DIFFERED_COLS + UNDERSCORE +DWVALIDATION_COL_SAMPLING_COUNT ;
								if(diffReportingColLimit.get(colName) == null){
									diffReportingColLimit.put(colName, 0);
								}else{
									currentValue = diffReportingColLimit.get(colName);
									if(currentValue<limitCount){
										differedCols.append(colName);
										differedCols.append(COMMA);
										differedCols.append(sourceColVal);
										differedCols.append(COMMA);
										differedCols.append(targeColVal);
										differedCols.append(COMMA);
										differedCols.append(key.toString());
										differedCols.append(COMMA);
										differedCols.append(sourceFileName);
										/*differedCols.append(COMMA);
										differedCols.append(targetFileName);	*/							
										keyOut.set(differedCols.toString());									
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
										differedCols.setLength(0);	
										diffReportingColLimit.put(colName, currentValue +1);
										int temp = diffReportingColLimit.get(colName);
										if(temp>10){
											throw new DWException("Looping more time" + Integer.toString( temp));
										}
										
									}
								}
							}
						}else{
							matchedCols.append(colName);
							matchedCols.append(SEMICOLON);
							targetHiveTableFlagMap.put(colName, true);
						}
					}else if(targetHiveTableMap.get(colName) ==null ){
						onlySourceCols.append(colName);
						onlySourceCols.append(TAB);
						onlySourceCols.append(sourceColVal);
						onlySourceCols.append(TAB);
						onlySourceCols.append(key.toString());
						onlySourceCols.append(TAB);
						onlySourceCols.append(sourceFileName);						
						keyOut.set(onlySourceCols.toString());
						String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS ;
						out.write(keyOut, NullWritable.get(), resultLocation);	
						onlySourceCols.setLength(0);
					///Printing limit output
						int currentValue =0;
						String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  EXISTS_ONLY_IN_SOURCE_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
						if(sourceReportingColLimit.get(colName) == null){
							sourceReportingColLimit.put(colName, 0);
						}else{
							currentValue = sourceReportingColLimit.get(colName);
							if(currentValue<limitCount){
								onlySourceCols.append(colName);
								onlySourceCols.append(COMMA);
								onlySourceCols.append(sourceColVal);
								onlySourceCols.append(COMMA);							
								onlySourceCols.append(sourceFileName);												
								keyOut.set(differedCols.toString());									
								out.write(keyOut, NullWritable.get(), smplingResultLocation);	
								onlySourceCols.setLength(0);	
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
							if(targeColVal=="null"){
								updatetoNull.append(colName);
								updatetoNull.append(TAB);
								updatetoNull.append(key.toString());								
							}else{
								onlyTargetCols.append(colName);
								onlyTargetCols.append(TAB);
								onlyTargetCols.append(targetFileName);
								keyOut.set(onlyTargetCols.toString());
								String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_TARGET_COLS;
								out.write(keyOut, NullWritable.get(), resultLocation);	
								onlyTargetCols.setLength(0);
								//prinitng limit output
								int currentValue =0;
								String smplingResultLocation = rootOutputLoc + FSEP + SAMPLING_FOLDER_NAME + FSEP +  EXISTS_ONLY_IN_TARGET_COLS + UNDERSCORE + DWVALIDATION_COL_SAMPLING_COUNT ;
								if(targetReportingColLimit.get(colName) == null){
									targetReportingColLimit.put(colName, 0);
								}else{
									currentValue = sourceReportingColLimit.get(colName);
									if(currentValue<limitCount){
										onlyTargetCols.append(colName);										
										onlySourceCols.append(COMMA);							
										onlySourceCols.append(targetFileName);												
										keyOut.set(differedCols.toString());									
										out.write(keyOut, NullWritable.get(), smplingResultLocation);	
										onlySourceCols.setLength(0);	
										targetReportingColLimit.put(colName, currentValue +1);
										int temp = targetReportingColLimit.get(colName);
										if(temp>limitCount){
											throw new DWException("Looping more time" + Integer.toString( temp));
										}
										
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
					onlySourceCols.append(colName);
					onlySourceCols.append(TAB);	
					onlySourceCols.append(sourceColVal);
					onlySourceCols.append(TAB);
					onlySourceCols.append(key.toString());
					onlySourceCols.append(TAB);
					onlySourceCols.append(sourceFileName);					
					keyOut.set(onlySourceCols.toString());
					String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS + "FromDepth";
					out.write(keyOut, NullWritable.get(), resultLocation);	
					onlySourceCols.setLength(0);
				}
			}else if (itrDepth==1 && targetTableExist){
				//result = key.toString() + COLON + "<Not in comparision Date range>" +PIPE + " ReduceSideFile:" + fileNameHolders.toString(); 
				//context.write(new Text(result), NullWritable.get());
				//result = key.toString() + "::" + record ;
				//result = key.toString()  ;
				//context.write(new Text(result), NullWritable.get());
			}
		}catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}

	}

	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		//System.out.println("Required Tables:" + srcRequiredTable);
		out.close();

	}
}
