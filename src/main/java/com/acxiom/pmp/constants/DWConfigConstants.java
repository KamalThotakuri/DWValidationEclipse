package com.acxiom.pmp.constants;

import java.io.File;

public interface DWConfigConstants {
	// Property file constants
	String DWVALIDATION_START_DATAE = "dwvalidation.start.date";
	String DWVALIDATION_END_DATAE =  "dwvalidation.end.date";
	String DWVALIDATION_SOURCE_TABLES_DATA_LOCATON="dwvalidation.source.tables.data.location";
	String DWVALIDATION_COMPARISION_LEVEL="dwvalidation.comparision.level";
	String DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE="dwvalidation.source.tables.required.tocompare";
	String DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE= "dwvalidation.target.hive.table.tocompare";
	String DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON="dwvalidation.target.dw.table.data.location";
	String DWVALIDATION_RESULT_LOCATION="dwvalidation.result.location";
	String DWVALIDATION_TARGET_HEADER="dwvalidation.target.header";
	String DWVALIDATION_SOURCE_HEADERS = "dwvalidation.headers";
	String DWVALIDATION_ROW_KEY="dwvalidation.row.key";
	String DWVALIDATION_COL_SAMPLING_COUNT="dwvalidation.col.sampling.cout";
	String DWVALIDATION_SOURCE_EXCLUDED_TABLES="dwvalidation.source.excluded.tables";
	String DWVALIDATION_SOURCE_QUOTES_TABLES = "dwvalidation.source.quotes.tables";
	// Utility Constants 
	String DATE_COL_INDEXS="0";
	String TABLE_NAME_SPLITTER_FROM_FNAME="_1TIME_";
	String COMMA = ",";
	String FSEP = File.separator;
	String LSEP = System.getProperty("line.separator");
	String APPENDER = "\u00FF\u00FF";
	String SEMICOLON = ";";
	String TAB = "\t";
	String TILD = "~";
	String EQUALS = "=";
	String DOT = "\\.";
	String UNDERSCORE = "_";
	String FOPBRACKET = "{";
	String FCLBRACKET = "}";
	String PIPE = "|";
	String ADDITIONAL_NEWLINE= ",newline";
	String COMPARISION_FULL="FULL";
	String OPBRACKET = "[";
	String CLBRACKET = "]";
	String DIFFERED_COLS="differed_cols";
	String MERGED_DIFFERED_COLS="merged_differed_cols_result.tsv";
	String MATCHED_COLS="matched_cols";
	String EXISTS_ONLY_IN_SOURCE_COLS="exists_only_in_source_cols";
	String MERGED_EXISTS_ONLY_IN_SOURCE_COLS="merged_exists_only_in_source_cols.tsv";
	String EXISTS_ONLY_IN_TARGET_COLS="exists_only_in_target_cols";
	String MERGED_EXISTS_ONLY_IN_TARGET_COLS="merged_exists_only_in_target_cols.tsv";
	String NOT_UPDATED_TO_NULL="not_updated_to_null";
	String SAMPLING_FOLDER_NAME= "LIMITBY";
	String DATE_COL_REFERENCE= "_dt";
	String DATE_COL_REFERENCE_1="_date";
	String HYPHEN="-";
	String EMPTY = "";
	String QUOTES="\"";
	String SINGLE_COLON=":";
	// conf file identifier  Constants 
	String PIN="dwconfig.pin.properties";
	String BIN="dwconfig.bin.properties";
	String PBL="dwconfig.pbl.properties";
	String ADDRESS="dwconfig.address.properties";
	String ZIP9="dwconfig.zip.properties";
	
	//Colms Mapping for BIN
	
	String UPDT_DT="_UPDT_DT";
	String UPT_DT="_UPT_DT";
	String UPDATE_DATE="_UPDATE_DATE";
	String UPD_DT="_UPD_DT";
	
	//Mapper Constant
	String ORC_NULL= "\\N";
	String THORN="\u00FE";
	String SFTYPE="source";
	String TFTYPE="targert";
	
			
	


}
