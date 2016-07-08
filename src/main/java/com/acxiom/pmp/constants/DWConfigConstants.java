package com.acxiom.pmp.constants;

import java.io.File;

public interface DWConfigConstants {
	// Property file constants
	String DWVALIDATION_START_DATAE = "dwvalidation.start.date";
	String DWVALIDATION_END_DATAE =  "dwvalidation.end.date";
	String DWVALIDATION_SOURCE_TABLES_DATA_LOCATON="dwvalidation.source.tables.data.location";
	String DWVALIDATION_COMPRESSION_LEVEL="dwvalidation.comparision.level";
	String DWVALIDATION_SOURCE_TABLES_REQUIRED_TOCOMPARE="dwvalidation.source.tables.required.tocompare";
	String DWVALIDATION_TARGET_HIVE_TABLE_TOCOMPARE= "dwvalidation.target.hive.table.tocompare";
	String DWVALIDATION_TARGET_DW_TABLE_DATA_LOCATON="dwvalidation.target.dw.table.data.location";
	String DWVALIDATION_RESULT_LOCATION="dwvalidation.result.location";
	String DWVALIDATION_TARGET_HEADER="dwvalidation.target.header";
	String DWVALIDATION_SOURCE_HEADERS = "dwvalidation.headers";
	String DWVALIDATION_ROW_KEY="dwvalidation.row.key";
	String DWVALIDATION_COL_SAMPLING_COUNT="dwvalidation.col.sampling.cout";
	String DWVALIDATION_SOURCE__EXCLUDED_TABLES="dwvalidation.source.excluded.tables";
	// Utility Constants 
	String DATE_COL_INDEXS="0";
	String TABLE_NAME_SPLITTER_FROM_FNAME="_1TIME_";
	String COMMA = ",";
	String FSEP = File.separator;
	String LSEP = System.getProperty("line.separator");
	String COLON = ":::::";
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
	String COMPRESSION_FULL="FULL";
	String OPBRACKET = "[";
	String CLBRACKET = "]";
	String DIFFERED_COLS="differed_cols";
	String EXISTS_ONLY_IN_SOURCE_COLS="exists_only_in_source_cols";
	String EXISTS_ONLY_IN_TARGET_COLS="exists_only_in_target_cols";
	String NOT_UPDATED_TO_NULL="not_updated_to_null";
	String SAMPLING_FOLDER_NAME= "LIMITBY";
	String DATE_COL_REFERENCE= "_dt";
	String HYPHEN="-";

}
