package com.acxiom.pmp.mr.dataloadvalidation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.constants.DWConfigConstants;
//Comment added 
public class ValidatorMapperBak extends Mapper<LongWritable, Text, Text, Text > implements DWConfigConstants{
	private String date;
	private String tableName;
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable; 
	private int primaryKeyIndex=0;
	private boolean isCompositeKey = false;
	private String compositeKey;
	//Has to remove the below variables and make them local
	private String inputFilePath;
	private String inputFileName;
	private String sourceDataLocation;
	private String lstName;
	private static Logger log = LoggerFactory.getLogger(ValidatorMapperBak.class);

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		try {
			inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();	
			sourceDataLocation = conf.get(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON);
			targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
			//$(Prefix)_1TIME_DATA_YYYYMMDD.tsv
			//SBKTO::
			//InputFile Path:maprfs:///mapr/thor/amexprod/STAGING/tempdelete/srcTableDir/Data/20160531/BIN/SBKTO_1TIME_DATA_20160531.tsv 
			//InputFileName:SBKTO_1TIME_DATA_20160531.tsv 
			//sourceDataLocation:/mapr/thor/amexprod/STAGING/tempdelete/srcTableDir/::
			if(inputFilePath.contains(sourceDataLocation)){
				String[] nameHolder = inputFileName.split(UNDERSCORE);
				tableName = nameHolder[0];
				int index = nameHolder.length-1;
				lstName = nameHolder[index];
				//lastName:20160531.tsv
				String[] dateHolder = lstName.split(DOT);
				date = dateHolder[0];
			}else{
				tableName = targetHiveTable;
			}

		} catch (Exception e1) {
			e1.printStackTrace();
		}  
	}

	class DataRecord {
		String primaryKey;
		String colsWithoutPKey;

		public DataRecord(String primaryKey, String colsWithoutPKey) {
			this.primaryKey = primaryKey;
			this.colsWithoutPKey = colsWithoutPKey;
		}

		public String getPrimaryKey() {
			return primaryKey;
		}

		public String getColsWithoutPKey() {
			return colsWithoutPKey;
		}
	}

	private DataRecord handlePrimarKey(Text value) {

		String[] columns = value.toString().split(TAB);
		String primaryKey = columns[primaryKeyIndex];

/*		StringBuilder result = new StringBuilder();
		for(int colIdx=0; colIdx<columns.length; colIdx++) {

			if(colIdx == primaryKeyIndex) {
				continue;
			}
			result.append(columns[colIdx].trim()+TAB);
		}
		if(result.length() > 0) {
			result.setLength(result.length()-1);
		}
		return new DataRecord(primaryKey, result.toString());*/
		return new DataRecord(primaryKey, value.toString());
		
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

		DataRecord record = null;
		if(isCompositeKey) {
			record = handleCompositeKey(value);
		} else {
			record = handlePrimarKey(value);
		}
		// 
		StringBuilder sb = new StringBuilder();
		sb.append(tableName);
		sb.append(COLON);

		if (!tableName.equals(targetHiveTable)){
			//This line has to keep
			sb.append(date);

			//sb.append(" Date:" + date + "targetHiveTable :" + targetHiveTable);
		}else{
			sb.append("yyyymmdd");
		}
		sb.append(COLON);
		sb.append(record.getColsWithoutPKey());

		// bin is 1st column
		keyOut.set(record.getPrimaryKey());
		valueOut.set(sb.toString());
		log.info("Primary Key:" + record.getPrimaryKey());
		System.out.println("Primary Key:" + record.getPrimaryKey());
		context.write(keyOut, valueOut);


	}

	private DataRecord handleCompositeKey(Text value) {
		return null;
	}
}
