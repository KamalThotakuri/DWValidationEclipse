package com.acxiom.pmp.mr.dataloadvalidation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;

public class ValidatorReducerBak extends Reducer< Text, Text, Text, NullWritable> implements DWConfigConstants {
	private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
	private static Logger log = LoggerFactory.getLogger(ValidatorReducerBak.class);

	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable;
	private String csTargetHeader;
	private String srcRequiredTable;


	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		String headerFiles = conf.get(DWVALIDATION_SOURCE_HEADERS);
		targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
		csTargetHeader = conf.get(DWVALIDATION_TARGET_HEADER);
		srcRequiredTable = conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE);
		map = DWUtil.getHeadersAsMap(headerFiles);
	}


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException{

		Map<String, String> targetHiveTableMap = new HashMap<String, String>();
		Map<String, String> soruceTableMap = new HashMap<String, String>();
		Map<String, Boolean> targetHiveTableFlagMap = new HashMap<String, Boolean>();
		String tableName=null;
		String matchedColsHolder=null;
		String differedColssHolder=null;
		String existsOnlyinSource=null;
		String existsOnlyinTarget=null;
		String result=null;
		StringBuilder onlySourceCols = new StringBuilder();
		StringBuilder onlyTargetCols = new StringBuilder();
		StringBuilder matchedCols = new StringBuilder();
		StringBuilder differedCols = new StringBuilder();
		String date = "";


		//tempdelete
		StringBuilder soruceMap = new StringBuilder();
		StringBuilder targetMap = new StringBuilder();
		String record=null;

		System.out.println("Reducer Starts");
		log.info("reducer messaged from log4j");
		int itrDepth=0;
		boolean targetTableExist=false;
		for(Text tableDateData : values){

			String[] tableDateDataArr = tableDateData.toString().split(COLON,3);
			tableName = tableDateDataArr[0];
			date = tableDateDataArr[1];
			record = tableDateDataArr[2];
			if(!tableName.equals(targetHiveTable)){
				Map<String, String> dateHeader = map.get(tableName);
				String header = dateHeader.get(date);
				log.info("datevalue: "+date);	
				System.out.println("datevalue: "+date);
				String[] cols = header.split(COMMA);
				String[] colValues = record.split(TAB);
				for(int i = 0; i < cols.length; i++) {
					soruceTableMap.put(cols[i], colValues[i]);
				}
			}else {
				/// Loading of Target HashMap
				targetTableExist=true;
				String[] cols = csTargetHeader.split(COMMA);
				String[] colValues = record.split(TAB);
				for(int i = 0; i < cols.length; i++) {
					targetHiveTableMap.put(cols[i], colValues[i]);
					targetHiveTableFlagMap.put(cols[i], false);
				}
			}
			itrDepth ++;
		}

		try{
			if(itrDepth >1 && targetTableExist){
				for (String colName : soruceTableMap.keySet()) {
					String targetColValue = targetHiveTableMap.get(colName);
					String sourceColValue = soruceTableMap.get(colName);

					if(targetColValue == null){
						onlySourceCols.append(colName);
						onlySourceCols.append(";");
					}if(targetColValue.equals(sourceColValue)){
						matchedCols.append(colName);
						matchedCols.append(";");						
						targetHiveTableFlagMap.put(colName, true);
					}if(!(targetColValue.equals(sourceColValue))){
						differedCols.append("[");
						differedCols.append(colName);
						differedCols.append(":");
						differedCols.append(sourceColValue);
						differedCols.append(",");
						differedCols.append(targetColValue);
						differedCols.append(";");
						targetHiveTableFlagMap.put(colName, true);
					}
				}
				for(String colName: targetHiveTableFlagMap.keySet()){

					if(srcRequiredTable.contains(colName)){
						if(!(targetHiveTableFlagMap.get(colName))){
							onlyTargetCols.append(colName);
							onlyTargetCols.append(";");
						}
					}
				}
				if(matchedCols.length()>0){
					matchedCols.setLength(matchedCols.length()-1);
					matchedColsHolder = "MatchedColumns "+ FOPBRACKET + matchedCols.toString() + FCLBRACKET;
				}else{
					matchedColsHolder = "MatchedColumns "+ FOPBRACKET + matchedCols.toString() + FCLBRACKET;
				}
				if(differedCols.length()>0){
					differedCols.setLength(differedCols.length()-1);
					differedColssHolder= "DifferedColumns "+ FOPBRACKET + differedCols.toString() + FCLBRACKET;
				}else{
					differedColssHolder= "DifferedColumns "+ FOPBRACKET + differedCols.toString() + FCLBRACKET;
				}
				if(onlySourceCols.length()>0){
					onlySourceCols.setLength(onlySourceCols.length()-1);
					existsOnlyinSource= "ColumnsExistOnlyInSource "+ FOPBRACKET  + onlySourceCols.toString() + FCLBRACKET;
				}else{
					existsOnlyinSource= "ColumnsExistOnlyInSource "+ FOPBRACKET  + onlySourceCols.toString() + FCLBRACKET;
				}

				if(onlyTargetCols.length()>0){
					onlySourceCols.setLength(onlySourceCols.length()-1);
					existsOnlyinTarget= "ColumnsExistOnlyInHive "+ FOPBRACKET  + onlyTargetCols.toString() + FCLBRACKET;
				}else{
					existsOnlyinTarget= "ColumnsExistOnlyInHive "+ FOPBRACKET  + onlyTargetCols.toString() + FCLBRACKET;
				}

				//context.write(new Text(result), NullWritable.get());
				//result = soruceMap.toString() + "::" + targetMap.toString(); 
				//result = key.toString()  ;
				//result = soruceMap.toString();
				//result= targetMap.toString(); 
				result = key.toString() + COLON + matchedColsHolder + PIPE + differedColssHolder + PIPE + existsOnlyinSource + PIPE + existsOnlyinTarget ;
				context.write(new Text(result), NullWritable.get());

			}else if (itrDepth >=1 && !targetTableExist){
				for (String colName : soruceTableMap.keySet()) {
					onlySourceCols.append(colName);
					onlySourceCols.append(";");
				}
				if(onlySourceCols.length()>0){
					onlySourceCols.setLength(onlySourceCols.length()-1);
					existsOnlyinSource=  FOPBRACKET  + onlySourceCols.toString() + FCLBRACKET;
				}else{
					existsOnlyinSource=  FOPBRACKET  + onlySourceCols.toString() + FCLBRACKET;
				}
				result = key.toString() + COLON + "This key exists only in source tables and its columns are " + existsOnlyinSource; 
				context.write(new Text(result), NullWritable.get());

			}else if (itrDepth==1 && targetTableExist){
				result = key.toString() + COLON + "<Not in comparision Date range>"; 
				//context.write(new Text(result), NullWritable.get());
				//result = key.toString() + "::" + record ;
				//result = key.toString()  ;
				context.write(new Text(result), NullWritable.get());
			}
		}catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 




	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		System.out.println("Required Tables:" + srcRequiredTable);

	}
}