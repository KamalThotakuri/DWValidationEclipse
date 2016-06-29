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
	private static Logger log = LoggerFactory.getLogger(ValidatorReducer.class);

	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable;
	private String csTargetHeader;
	private String srcRequiredTable;
	private MultipleOutputs<Text, NullWritable> out;
	private String rootOutputLoc;

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		String headerFiles = conf.get(DWVALIDATION_SOURCE_HEADERS);
		targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
		csTargetHeader = conf.get(DWVALIDATION_TARGET_HEADER);
		srcRequiredTable = conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE);
		rootOutputLoc = conf.get(DWVALIDATION_RESULT_LOCATION);
		map = DWUtil.getHeadersAsMap(headerFiles);
		out = new MultipleOutputs(context);
	}


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException{

		Map<String, String> targetHiveTableMap = new HashMap<String, String>();
		Map<String, String> soruceTableMap = new HashMap<String, String>();
		Map<String, Boolean> targetHiveTableFlagMap = new HashMap<String, Boolean>();
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
						soruceTableMap.put(cols[i], colValues[i]);
						ss.append(colValues[i]);
						ss.append(";");
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
				targetTableExist=true;
				String[] cols = csTargetHeader.split(COMMA);
				String[] colValues = record.split(TAB,-2);
				try{
					for(int i = 0; i < cols.length; i++) {
						targetHiveTableMap.put(cols[i], colValues[i]);
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
			}
			itrDepth ++;
		}

		try{
			if(itrDepth >1 && targetTableExist){
				for (String colName : soruceTableMap.keySet()) {
					String targetColValue = targetHiveTableMap.get(colName);
					String sourceColValue = soruceTableMap.get(colName);

					if(targetColValue !=null){
						if(sourceColValue.equals(targetColValue)){
							matchedCols.append(colName);
							matchedCols.append(SEMICOLON);
							targetHiveTableFlagMap.put(colName, true);
						}else{

							differedCols.append(colName);
							differedCols.append(TAB);
							differedCols.append(sourceColValue);
							differedCols.append(TAB);
							differedCols.append(targetColValue);
							differedCols.append(TAB);
							differedCols.append(key.toString());
							differedCols.append(LSEP);
							keyOut.set(differedCols.toString());
							String resultLocation = rootOutputLoc + FSEP + DIFFERED_COLS;
							out.write(keyOut, NullWritable.get(), resultLocation);							
							targetHiveTableFlagMap.put(colName, true);
						}
					}else{
						onlySourceCols.append(colName);
						onlySourceCols.append(TAB);
						onlySourceCols.append(sourceColValue);
						onlySourceCols.append(TAB);
						onlySourceCols.append(key.toString());
						keyOut.set(onlySourceCols.toString());
						String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS;
						out.write(keyOut, NullWritable.get(), resultLocation);	
					}
				}
				for(String colName: targetHiveTableFlagMap.keySet()){
					/*String[] colTableNameHolder = colName.split(UNDERSCORE);
					String colTableName = colTableNameHolder[0];*/
					if(srcRequiredTable.contains(colName)){
						if(!(targetHiveTableFlagMap.get(colName))){
							if(targetHiveTableMap.get(colName)=="null"){
								updatetoNull.append(colName);
								updatetoNull.append(TAB);
							}else{
								onlyTargetCols.append(colName);
								onlyTargetCols.append(TAB);
								onlyTargetCols.append(targetHiveTableMap.get(colName));
								onlyTargetCols.append(TAB);
								onlyTargetCols.append(key.toString());
								keyOut.set(onlyTargetCols.toString());
								String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS;
								out.write(keyOut, NullWritable.get(), resultLocation);	
							}
							onlyTargetCols.append(colName);
							onlyTargetCols.append(TAB);
							onlyTargetCols.append(targetHiveTableMap.get(colName));
							onlyTargetCols.append(TAB);
							onlyTargetCols.append(key.toString());
							keyOut.set(onlyTargetCols.toString());
							String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS;
							out.write(keyOut, NullWritable.get(), resultLocation);	

						}
					}
				}

			}else if (itrDepth >=1 && !targetTableExist){
				for (String colName : soruceTableMap.keySet()) {
					onlySourceCols.append(colName);
					onlySourceCols.append(TAB);
					onlySourceCols.append(soruceTableMap.get(colName));
					keyOut.set(onlySourceCols.toString());
					String resultLocation = rootOutputLoc + FSEP + EXISTS_ONLY_IN_SOURCE_COLS;
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