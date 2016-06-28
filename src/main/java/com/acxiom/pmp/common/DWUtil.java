/**
 * 
 */
package com.acxiom.pmp.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acxiom.pmp.constants.DWConfigConstants;

/**
 * @author kthota
 *
 */
public class DWUtil implements DWConfigConstants {
	private static Logger log = LoggerFactory.getLogger(DWUtil.class);

	/*dwvalidation.start.date=20160511
//			dwvalidation.end.date=20160611
			dwvalidation.source.tables.data.location=/mapr/thor/amexprod/STAGING/1TIME/
			dwvalidation.comparision.level=full
			dwvalidation.source.tables.requried.tocompare=PBABI,PBDEN,PBALP,PBLBC,PBLCM,PBLCO,PBLDD,PBLCS,PBLEH,PBEMA,PBLEQ,PBLQB,PBLEX,PBBOL
			dwvalidation.target.hive.table.tocompare=PIN
			dwvalidation.target.dw.table.data.location=/mapr/thor/HIVE*/
	public static String parseSoruceTableInput(String startDate, String endDate, 
			String comparisionLevel, String sourceTableList, String targetTable, String sourceTableinputPath){
		FileSystem fs;
		StringBuilder requiredFolderList = new StringBuilder();
		StringBuilder requiredFileList = new StringBuilder();
		sourceTableinputPath = sourceTableinputPath + "Data" + FSEP  ;

		switch (comparisionLevel){
		case "FULL":	
			try {
				Path inFile = new Path(sourceTableinputPath);
				fs = FileSystem.get(new Configuration());
				FileStatus[] fileStatus = fs.listStatus(inFile);
				for(FileStatus status : fileStatus){
					if(!status.isDir()){
						continue ;
					}

					try {
						String dateFolder = status.getPath().getName();
						int currentDate = Integer.parseInt(dateFolder);
						if(currentDate >= Integer.parseInt(startDate) && currentDate <= Integer.parseInt(endDate)){
							requiredFolderList.append(status.getPath().toString() +  FSEP +targetTable+ COMMA);
						}
					} catch (Exception e) {
						e.printStackTrace();
						throw new DWException("Error occured while ", e);
					}
				}
				if(requiredFolderList.length()>0){
					requiredFolderList.setLength(requiredFolderList.length()-1);	
				}

			} catch (Exception e) {
				throw new DWException("Error occured while getting required source table list", e);
			}
			return requiredFolderList.toString();
		default:
			try {
				Path inFile = new Path(sourceTableinputPath);
				fs = FileSystem.get(new Configuration());
				FileStatus[] fileStatus = fs.listStatus(inFile);
				for(FileStatus status : fileStatus){
					if(!status.isDir()){
						continue ;
					}
					String dateFolder = status.getPath().getName();
					int currentDate = Integer.parseInt(dateFolder);
					if(currentDate >= Integer.parseInt(startDate) && currentDate <= Integer.parseInt(endDate)){
						//requiredFolderList.append(status.getPath().toString() +  FSEP +targetTable+ COMMA);
						FileStatus[] dataFileStatus = fs.listStatus(status.getPath());
						for(FileStatus dataFile:dataFileStatus){
							if(status.isDir()){
								continue ;
							}
							String inputPathFileName = status.getPath().getName();
							String[] fileSplitHoder = inputPathFileName.split(UNDERSCORE);
							String tablename = fileSplitHoder[0];
							if(sourceTableList.contains(tablename)){
								requiredFileList.append(dataFile.getPath().toString() + COMMA);
							}

						}
					}

				}
				if(requiredFileList.length()>0){
					requiredFileList.setLength(requiredFolderList.length()-1);	
				}

			} catch (Exception e) {
				throw new DWException("Error occured while getting required source table list", e);
			}
			return requiredFileList.toString();
		}

	}

	/*dwvalidation.target.hive.table.tocompare=PIN
			dwvalidation.target.dw.table.data.location=/mapr/thor/HIVE*/
	public static String parseDWTableInput(Configuration conf,String DWTableName, String DWTableDataLocation){

		FileSystem fs;
		StringBuilder requiredFolderList = new StringBuilder();

		try {
			Path inFile = new Path(DWTableDataLocation);
			fs = FileSystem.get(conf);
			FileStatus[] fileStatus = fs.listStatus(inFile);
			for(FileStatus status : fileStatus){

				if(status.isDir()){
					continue ;
				}
				requiredFolderList.append(status.getPath().toString() +  FSEP +DWTableName+ COMMA);
			}
			if(requiredFolderList.length()>0){
				requiredFolderList.setLength(requiredFolderList.length()-1);	
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return requiredFolderList.toString();
	}


	// B1=20160504:$(SERVER_NFS)/STAGING/1TIME/Header/20160504/BIN/$(B1)_1TIME_HEADER_YYYYMMDD.tsv,20160504:$(SERVER_NFS)/STAGING/1TIME/Header/20160505/BIN/$(B1)_1TIME_HEADER_YYYYMMDD.tsv, ...
	// $(SERVER_NFS)/STAGING/1TIME/Header/YYYYMMDD/
	/*
			dwvalidation.source.tables.data.location=/mapr/thor/amexprod/STAGING/1TIME/
			dwvalidation.start.date=20160511
			dwvalidation.end.date=20160611
			dwvalidation.target.hive.table.tocompare=PIN
			dwvalidation.target.dw.table.data.location=/mapr/thor/HIVE
			dwvalidation.result.location=
	 */


	// Input: $(SERVER_NFS)/STAGING/1TIME/Header/YYYYMMDD/
	// Output: B1=20160504:$(SERVER_NFS)/STAGING/1TIME/Header/20160504/BIN/$(B1)_1TIME_HEADER_YYYYMMDD.tsv,20160504:$(SERVER_NFS)/STAGING/1TIME/Header/20160505/BIN/$(B1)_1TIME_HEADER_YYYYMMDD.tsv, ...
	public static Properties getSourceHeaderFiles(String csSoruceTableInputStr) {
		Properties scHeaderconfig = new Properties();

		try {
			FileSystem fs = FileSystem.get(new Configuration());
			String[] inputDateFolders = csSoruceTableInputStr.split(COMMA);

			// traverse required date folders
			for(String inputDateFolder : inputDateFolders){

				Path currentDateFolder = new Path(inputDateFolder);
				// $(SERVER_NFS)/STAGING/1TIME/Header/YYYYMMDD/BIN to YYYYMMDD
				String dateValue = currentDateFolder.getParent().getName();

				Path inFile = new Path(inputDateFolder);
				FileStatus[] fileStatus = fs.listStatus(inFile);
				//:  $(SERVER_NFS)/STAGING/1TIME/Header/YYYYMMDD/BIN/B1.tsv,B2.tsv
				for(FileStatus status : fileStatus){

					if(status.isDir()){
						continue ;
					}
					//  B1=20160504:$(SERVER_NFS)/STAGING/1TIME/Header/20160504/BIN/$(B1)_1TIME_HEADER_YYYYMMDD.tsv
					String dataFilePath = status.getPath().toString();
					String headerFilePath = dataFilePath.replace("/Data/", "/Header/");
					//log.info("FileName : " + fileName);
					String fileName = status.getPath().getName(); // $(B1)_1TIME_HEADER_YYYYMMDD.tsv
					
					headerFilePath = headerFilePath.replace("_DATA_", "_HEADER_");
                  
					//$(Prefix)_1TIME_HEADER_YYYYMMDD.tsv
					String[] fileNameSplit = fileName.split("_");
					String sValue= dateValue + COLON + headerFilePath;
					String tableName = fileNameSplit[0];
					// first get the property to see if one exists on this key; if yes, then update it; else create new property
					String val = scHeaderconfig.getProperty(tableName);
					if(val == null) {
						// create
						scHeaderconfig.setProperty(tableName, sValue);
					} else {
						// update
						scHeaderconfig.setProperty(tableName, val+COMMA+sValue);
						
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new DWException("Failed to load the headers", e);
		}

		return scHeaderconfig;
	}

	
	// Output: 
	// delimiters are 
	// LSEP for lines
	// = for 1st map
	// ~ for 2nd map
	// : for 3rd map ( ',' in values)
	//Output
	//SBKTO=20160531::BIN,SBKTO_FLAG,SBKTO_UPD_DT,SBKTO_sitebin,SBKTO_pga_sales,SBKTO_pga_employees,SBKTO_pga_sic,SBKTO_pga_business_tenure,SBKTO_pga_legal_structure,SBKTO_pgnt,SBKTO_pga_csow_plasticizable,SBKTO_pga_csow_source,SBKTO_pga_casf
	//BONPS=20160531::BIN,BONPS_FLAG,BONPS_ADD_DT,BONPS_YEAR,BONPS_WEEK,BONPS_RESP_BPLAT_OPEN_NPA_LL,BONPS_RESP_SC_OPEN_NPA_LL,BONPS_RESP_BGR_OPEN_NPA_LL
	public static String getSourceHeaderColumns(Properties scHeaderconfig) {

		//
		StringBuilder result = new StringBuilder();
		for(Entry<Object, Object> entry: scHeaderconfig.entrySet()) {
			String tableName = (String) entry.getKey();
			String csTableDateHeaders = (String) entry.getValue();
			
			String[] tableDateHeaders = csTableDateHeaders.split(COMMA);
			
			StringBuilder dateHeadersStr = new StringBuilder();
			for(String dateHeader: tableDateHeaders) {
				String[] dateHeaderArr = dateHeader.split(COLON);
				String date = dateHeaderArr[0];
				String headerFile = dateHeaderArr[1];
				
				// read the file
				String tsHeaderContent = "";
				System.out.println("Source HeaderFilePath" + headerFile + LSEP);
				try {
					FileSystem fs = FileSystem.get(new Configuration());
					FSDataInputStream is = fs.open(new Path(headerFile));
					BufferedReader br = new BufferedReader(new InputStreamReader(is));
					tsHeaderContent = br.readLine();
				} catch (Exception e) {
					e.printStackTrace();
				}

				String csHeaderContent = tsHeaderContent.replaceAll(TAB, COMMA);
				if(csHeaderContent.contains(",newline")){
					csHeaderContent = csHeaderContent.replaceAll(",newline", "");					
				}
				dateHeadersStr.append(date+COLON+csHeaderContent+TILD);
			}
			// remove last delimiter
			if(dateHeadersStr.length() > 0) {
				dateHeadersStr.setLength(dateHeadersStr.length()-1);
			}

			result.append(tableName+EQUALS+dateHeadersStr.toString()+LSEP);
		}
		return result.toString();
	}

	
	public static String getFullSrcTablesList(Properties scHeaderconfig){
		StringBuilder result = new StringBuilder();
		for(Entry<Object, Object> entry: scHeaderconfig.entrySet()) {
			String tableName = (String) entry.getKey();
			result.append(tableName);
			result.append(COMMA);
		}
		if(result.length() > 0) {
			result.setLength(result.length()-1);
		}
		return result.toString();
	}
	
	// Input: 
	// delimiters are 
	// LSEP for lines
	// = for 1st map
	// ~ for 2nd map
	// : for 3rd map ( ',' in values)
	//SBKTO=20160531::BIN,SBKTO_FLAG,SBKTO_UPD_DT,SBKTO_sitebin,SBKTO_pga_sales,SBKTO_pga_employees,SBKTO_pga_sic,SBKTO_pga_business_tenure,SBKTO_pga_legal_structure,SBKTO_pgnt,SBKTO_pga_csow_plasticizable,SBKTO_pga_csow_source,SBKTO_pga_casf
	//BONPS=20160531::BIN,BONPS_FLAG,BONPS_ADD_DT,BONPS_YEAR,BONPS_WEEK,BONPS_RESP_BPLAT_OPEN_NPA_LL,BONPS_RESP_SC_OPEN_NPA_LL,BONPS_RESP_BGR_OPEN_NPA_LL

	public static Map<String, Map<String, String>> getHeadersAsMap(String headersStr) {

		
		Map<String, Map<String, String>> secondMap = new HashMap<String, Map<String, String>>();
		//Map<String, Map<String, Map<String, String>>> firstMap = new HashMap<String, Map<String, Map<String, String>>>();

		String[] tablesDateHeaders = headersStr.split(LSEP);
		for(String tableDateHeader: tablesDateHeaders) {
			Map<String, String> thirdMap = new HashMap<String, String>();
			String[] tableDateHeaders = tableDateHeader.split(EQUALS);
			String tableName = tableDateHeaders[0];
			String dateHeadersStr = tableDateHeaders[1];
			String[] dateHeaders = dateHeadersStr.split(TILD);
			for(String dateHeaderStr: dateHeaders) {
				String[] dateHeader = dateHeaderStr.split(COLON);
				String date = dateHeader[0];
				String colsStr = dateHeader[1];
				thirdMap.put(date, colsStr);
				/*String[] cols = colsStr.split(COMMA);
				 * for(String col: cols) {
					thirdMap.put(date, col);
				}*/
			}
			secondMap.put(tableName, thirdMap);
		}

		return secondMap;
	}

	public static String getStackTraceAsString(Throwable e) {
		// TODO Auto-generated method stub
		Writer result = new StringWriter();
		PrintWriter printWriter = new PrintWriter(result);
		e.printStackTrace(printWriter);
		return result.toString();

	}

	public static String getTargetHeaderColumns(String targetBAUDWLocation, String targetHiveTableName) {		

		//targetBAUDWLocation = targetBAUDWLocation +FSEP +  "Header" + FSEP+ targetHiveTableName + FSEP ;	
		//VenkatData Testing
		targetBAUDWLocation = "/mapr/thor/amexprod/STAGING/tempdelete/targetHiveDir/Header/PBL/" ;
		Path headerFileLocation = null;
		String tsHeaderContent = "";
		try {
			Path inFile = new Path(targetBAUDWLocation);
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] fileStatus = fs.listStatus(inFile);
			for(FileStatus status : fileStatus){
				if(status.isDir()){
					continue ;
				}
				
				headerFileLocation = status.getPath();
			}
			FSDataInputStream is = fs.open(headerFileLocation);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			tsHeaderContent = br.readLine();
		} catch (Exception e) {
			e.printStackTrace();
		}
		String targetHeaderCols = tsHeaderContent.replaceAll(TAB, COMMA);
		return targetHeaderCols;
	}

}