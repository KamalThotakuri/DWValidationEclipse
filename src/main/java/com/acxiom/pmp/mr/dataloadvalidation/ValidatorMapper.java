
package com.acxiom.pmp.mr.dataloadvalidation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.compress.utils.Charsets;
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
	private String quotetables=null;
	private static Logger log = LoggerFactory.getLogger(ValidatorMapperBak.class);

	@Override
	protected void setup(Context context){
		Configuration conf = context.getConfiguration();
		try {
			inputFilePath = ((FileSplit) context.getInputSplit()).getPath().toString();
			inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();	
			quotetables = conf.get(DWVALIDATION_SOURCE_QUOTES_TABLES);
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
			StringBuilder result = new StringBuilder();
			String primaryKey = null;
			if(!inputFilePath.contains(sourceDataLocation)){
				//if(tableName.equals(targetHiveTable) ){
				//String[] columns = value.toString().split(THORN,-2);
				String data = new String(value.getBytes(),
						0, value.getLength(), 
						Charsets.ISO_8859_1);
				String[] columns = data.split(THORN,-2);
				primaryKey = columns[primaryKeyIndex];        
				String[] indx = dateColIndxs.split(COMMA);
				for(String in:indx){
					int dtindx = Integer.parseInt(in);
					String temp = columns[dtindx];
					columns[dtindx] = temp.replace(HYPHEN, "");
				}
				for(int colIdx=0; colIdx<columns.length; colIdx++) {
					result.append(columns[colIdx].trim()+THORN);
				}
			}else{
				String line = value.toString();
				String[] columns = line.split(TAB,-2);
				primaryKey = columns[primaryKeyIndex];   
				if(quotetables != null && quotetables.contains(tableName)){					
					if(line.contains(QUOTES)){
						String[] qtindex= quotetables.split(SINGLE_COLON);
						int qtpoint = Integer.parseInt(qtindex[1]) -2; 
						for(int i=0 ; i <=qtpoint ;i++ ){
							result.append(columns[i]);
							result.append(TAB);
						}
						line = line.replace(result.toString(), "");	
						result.setLength(0);
						for(int i=0 ; i <=qtpoint ;i++ ){
							result.append(columns[i]);
							result.append(THORN);
						}
						int loccurnace = line.lastIndexOf(QUOTES);
						String quotestring = line.substring(0, loccurnace+1);
						//quotestring = quotestring.replace(TAB, THORN);
						String secondset = line.substring(loccurnace +1);
						secondset = secondset.replace(TAB, THORN);
						result.append(quotestring);
						result.append(secondset);
					}else{
						for(int colIdx=0; colIdx<columns.length; colIdx++) {
							result.append(columns[colIdx].trim()+THORN);
						}
					}
				}else{
					for(int colIdx=0; colIdx<columns.length; colIdx++) {
						result.append(columns[colIdx].trim()+THORN);
					}
				}
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
		//String[] columnValues = value.toString().split(TAB,-2);
		int targetsplicount=0; 
		String srecord=null;
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
			sb.append(APPENDER);

			if (inputFilePath.contains(sourceDataLocation)){
				//This line has to keep
				sb.append(date);

				//sb.append(" Date:" + date + "targetHiveTable :" + targetHiveTable);
			}else{
				sb.append("yyyymmdd");
			}
			sb.append(APPENDER);		
			sb.append(inputFilePath);
			sb.append(APPENDER);
			//sb.append(columnValues.length);
			//sb.append(COLON);
			//sb.append(value.toString());
			sb.append(record.getRecord());
			sb.append(APPENDER);
			if(inputFilePath.contains(sourceDataLocation)){
				sb.append(SFTYPE);
			}else{
				sb.append(TFTYPE);
			}
			keyOut.set(record.getRowKey());
			valueOut.set(sb.toString());
			context.write(keyOut, valueOut);
			/*if(!tableName.equals(targetHiveTable) ){				
				srecord = record.getRecord();
				throw new Exception();
			}*/
			srecord = record.getRecord();
		}catch(Exception e){
			String exTrace = DWUtil.getStackTraceAsString(e);
			log.error("Error occured while sending the mapper output. "+e.getMessage());
			log.error(exTrace);
			throw new DWException("Record:" + srecord + FSEP + "File" + inputFilePath  ,e);
		}

	}
	private DataRecord handleCompositeKey(Text value) {
		StringBuilder combiner = new StringBuilder();
		StringBuilder result = new StringBuilder();


		if(!inputFilePath.contains(sourceDataLocation)){
			//String[] columns = value.toString().split(THORN,-2);
			String data = new String(value.getBytes(),
					0, value.getLength(), 
					Charsets.ISO_8859_1);
			String[] columns = data.split(THORN,-2);
			String[] indx = dateColIndxs.split(COMMA);
			for(Integer index:compositeKeyIndex){
				String cloName = columns[index];
				combiner.append(cloName);
				combiner.append(APPENDER);
			}
			for(String in:indx){
				int dtindx = Integer.parseInt(in);
				String temp = columns[dtindx];
				columns[dtindx] = temp.replace(HYPHEN, "");
			}
			
			if(combiner.length() > 0) {
				combiner.setLength(combiner.length()-1);
			}
			for(int colIdx=0; colIdx<columns.length; colIdx++) {
				result.append(columns[colIdx].trim()+THORN);
			}

		}else{
			//String[] columns = value.toString().split(THORN,-2);
			String line = value.toString();
			String[] columns = line.split(TAB,-2);

			for(Integer index:compositeKeyIndex){
				String cloName = columns[index];
				combiner.append(cloName);
				combiner.append(APPENDER);
			}
			if(combiner.length() > 0) {
				combiner.setLength(combiner.length()-1);
			}
			if(quotetables != null && quotetables.contains(tableName)){					
				if(line.contains(QUOTES)){
					String[] qtindex= quotetables.split(SINGLE_COLON);
					int qtpoint = Integer.parseInt(qtindex[1]) -2; 
					for(int i=0 ; i <=qtpoint ;i++ ){
						result.append(columns[i]);
						result.append(TAB);
					}
					line = line.replace(result.toString(), "");	
					result.setLength(0);
					for(int i=0 ; i <=qtpoint ;i++ ){
						result.append(columns[i]);
						result.append(THORN);
					}
					int loccurnace = line.lastIndexOf(QUOTES);
					String quotestring = line.substring(0, loccurnace+1);
					//quotestring = quotestring.replace(TAB, THORN);
					String secondset = line.substring(loccurnace +1);
					secondset = secondset.replace(TAB, THORN);
					result.append(quotestring);
					result.append(secondset);
				}else{
					for(int colIdx=0; colIdx<columns.length; colIdx++) {
						result.append(columns[colIdx].trim()+THORN);
					}
				}
			}else{
				for(int colIdx=0; colIdx<columns.length; colIdx++) {
					result.append(columns[colIdx].trim()+THORN);
				}
			}
		}

		String rowKey = combiner.toString();
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
