
package com.acxiom.pmp.mr.dataloadvalidation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.common.DWException;
import com.acxiom.pmp.common.DWUtil;
import com.acxiom.pmp.constants.DWConfigConstants;
//Comment added 
public class ValidatorMapper extends Mapper<LongWritable, Text, Text, Text > implements DWConfigConstants{
	private String date;
	private String tableName;
	private Text keyOut = new Text();
	private Text valueOut = new Text();
	private String targetHiveTable; 
	private int primaryKeyIndex=0;
	private boolean isCompositeKey = false;
	private String srcRequiredTable;
	private String rowKeyCols;
	private Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
	private Map<String, Map<String, String>> dateColIndxMap = new HashMap<String, Map<String, String>>();
	private ArrayList<Integer> compositeKeyIndex = new ArrayList<Integer>();
	private String dateColIndxs;	
	private String inputFilePath;
	private String inputFileName;
	private String sourceDataLocation;
	private String lstName;
	private static Logger log = LoggerFactory.getLogger(ValidatorMapper.class);

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		try {
			inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();	
			sourceDataLocation = conf.get(DWVALIDATION_SOURCE_TABLES_DATA_LOCATON);
			targetHiveTable = conf.get(DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE);
			srcRequiredTable = conf.get(DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE);
			String headerFiles = conf.get(DWVALIDATION_SOURCE_HEADERS);
			dateColIndxs = conf.get(DATE_COL_INDEXS);
			map = DWUtil.getHeadersAsMap(headerFiles);
			rowKeyCols = conf.get(DWVALIDATION_ROW_KEY);
			if(rowKeyCols.split(COMMA).length >1){
				isCompositeKey=true;
			}
			if(inputFilePath.contains(sourceDataLocation)){
				String[] nameHolder = inputFileName.split(UNDERSCORE);
				String[] tableNameHolder = inputFileName.split(TABLE_NAME_SPLITTER_FROM_FNAME);
				tableName = tableNameHolder[0];
				int index = nameHolder.length-1;
				lstName = nameHolder[index];
				//lastName:20160531.tsv
				String[] dateHolder = lstName.split(DOT);
				date = dateHolder[0];
				if(!isCompositeKey){
					Map<String, String> dateHeader = map.get(tableName);
					String header = dateHeader.get(date);
					String[] cols = header.split(COMMA);	
					for(int i = 0; i < cols.length; i++) {
						if(cols[i].equals(rowKeyCols)){
							primaryKeyIndex= i;
						}
					}
				}else{
					String[] keyColumns = rowKeyCols.split(COMMA);
					Map<String, String> dateHeader = map.get(tableName);
					String header = dateHeader.get(date);
					String[] cols = header.split(COMMA);	
					String[] keyCols = rowKeyCols.split(COMMA);
					for(String kcol:keyCols){
						innerloop:
							for(int i = 0; i < cols.length; i++) {
								if(cols[i].equals(kcol)){
									compositeKeyIndex.add(i);
									//break innerloop;
								}
							}
					}
				}	
			}else{
				tableName = targetHiveTable;
			}
		} catch (Exception e1) {
			e1.printStackTrace();
		}  
	}

	class DataRecord {
		String rowKey;
		String completeRecord;

		public DataRecord(String primaryKey, String completeRecord) {
			this.rowKey = primaryKey;
			this.completeRecord = completeRecord;
		}

		public String getRowKey() {
			return rowKey;
		}

		public String getRecord() {
			return completeRecord;
		}
	}

	private DataRecord handlePrimarKey(Text value) {
		try{
			String[] columns = value.toString().split(TAB,-2);
			if(tableName.equals(targetHiveTable) ){
				String[] indx = dateColIndxs.split(COMMA);
				for(String in:indx){
					int dtindx = Integer.parseInt(in);
					String temp = columns[dtindx];
					columns[dtindx] = temp.replace(HYPHEN, "");
				}
			}

			String primaryKey = columns[primaryKeyIndex];         
			StringBuilder result = new StringBuilder();
			for(int colIdx=0; colIdx<columns.length; colIdx++) {
				result.append(columns[colIdx].trim()+TAB);
			}
			if(result.length() > 0) {
				result.setLength(result.length()-1);
			}
			return new DataRecord(primaryKey, result.toString());

		}catch(Exception e){
			throw new DWException("From catch:", e);
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//TempDelete
		String[] columnValues = value.toString().split(TAB,-2);

		try{	
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
			sb.append(inputFilePath);
			sb.append(COLON);
			sb.append(columnValues.length);
			sb.append(COLON);
			//sb.append(value.toString());
			sb.append(record.getRecord());
			keyOut.set(record.getRowKey());
			valueOut.set(sb.toString());
			context.write(keyOut, valueOut);
		}catch(Exception e){
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while sending the mapper output. "+e.getMessage());
			log.error(exTrace);
			throw new DWException("Error occured while sending mapper output Filt Path is +" + inputFilePath,e);
		}

	}
	private DataRecord handleCompositeKey(Text value) {
		StringBuilder combiner = new StringBuilder();
		String[] columns = value.toString().split(TAB,-2);
		if(tableName.equals(targetHiveTable) ){
			String[] indx = dateColIndxs.split(COMMA);
			for(String in:indx){
				int dtindx = Integer.parseInt(in);
				String temp = columns[dtindx];
				columns[dtindx] = temp.replace(HYPHEN, "");
			}
		}
		for(Integer index:compositeKeyIndex){
			String cloName = columns[index];
			combiner.append(cloName);
			combiner.append(COLON);
		}
		if(combiner.length() > 0) {
			combiner.setLength(combiner.length()-1);
		}
		String rowKey = combiner.toString();
		StringBuilder result = new StringBuilder();
		for(int colIdx=0; colIdx<columns.length; colIdx++) {
			result.append(columns[colIdx].trim()+TAB);
		}
		if(result.length() > 0) {
			result.setLength(result.length()-1);
		}

		/*String cValues = value.toString();
		while (cValues.endsWith("\t")) {
			cValues = cValues.substring(0, cValues.length()-1);
		}*/
		return new DataRecord(rowKey, result.toString());
	}

}
