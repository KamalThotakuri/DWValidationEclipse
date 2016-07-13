#!/bin/bash

# command to invoke 
# ./run_queries.sh ./lib/csv-serde-1.1.2-0.11.0-all.jar ./lib/ngrams-1.0.jar ./users ./tweets ./stop_words/ edureka

echo $# arguments passed
if [ $# -ne 3 ]; then 
    echo "Illegal number of parameters, this script requires 6 arguments <hive_csv_serde_jar> <udf_jar> <users_data_dir> <tweets_data_dir> <stop_words_file> <queue_name>"
	exit 1
fi

tables_to_validate=$1
bawdw_table_extract_location=$2
queue_name=$3
IFS=,
ary=($tables_to_validate)
# use current directory as output


#***************************************** Start of BIN table ***************************************************************
# extract BIN table data 
flat=-1
current_table=BIN
binfolder=Data/BIN
extractlocation ="$bawdw_table_extract_location$binfolder"
for key in "${!ary[@]}"; do
 if [[ $current_table -eq ${ary[$key]} ]]; then
	 hdfs dfs -rmr $extractlocation
	 if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hdfs dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
     hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f ./scripts/extract_business_table_to_text.hql
	if [ $? -ne 0 ]; then
			echo "failed to extract BIN table data, exiting the script"
			exit -1
	fi
		# MR job for  BIN table data validation
		hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $current_table

		if [ $? -ne 0 ]; then
			echo "failed to execute the validation of BIN table, exiting the script"
			exit -1
		fi
    break  
 fi
done

#***************************************** end for BIN table *****************************************************************


#***************************************** Start of PERSON table *************************************************************

flat=-1
current_table=PIN
pinfolder=Data/PIN
extractlocation="$bawdw_table_extract_location$pinfolder"
for key in "${!ary[@]}"; do
 if [[ $current_table -eq ${ary[$key]} ]]; then
 	 if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hdfs dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
	# extract PERSON table data 
	hive -hiveconf users=$inputablename -hiveconf queue=$queue_name -f ./scripts/extract_person_table_to_text.hql
	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $current_table

	if [ $? -ne 0 ]; then
		echo "failed to execute the validation of PERSON table, exiting the script"
		exit -1
	fi
    break  
 fi
done
#***************************************** end for PERSON table ****************************************************************


#***************************************** Start of PERSON_AT_BUSINESS table ****************************************************
flat=-1
current_table=PBL
pblfolder=Data/PBL
extractlocation ="$bawdw_table_extract_location$pblfolder"

for key in "${!ary[@]}"; do
 if [[ $current_table -eq ${ary[$key]} ]]; then
 	 if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hdfs dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
	# extract PBL table data 
	hive -hiveconf users=$inputablename -hiveconf queue=$queue_name -f ./scripts/extract_person_at_business_table_to_text.hql

	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $current_table

	if [ $? -ne 0 ]; then
		echo "failed to execute the validation of PBL table, exiting the script"
		exit -1
	fi
    break  
 fi
done


#***************************************** end for PERSON_AT_BUSINESS table ******************************************************


#***************************************** Start of adress table *****************************************************************

flat=-1
current_table=ADDRESS
adressfolder=Data/ADDRESS
extractlocation ="$bawdw_table_extract_location$adressfolder"
for key in "${!ary[@]}"; do
 if [[ $current_table -eq ${ary[$key]} ]]; then
 	
	if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hdfs dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
# extract ADDRESS table data 
	hive -hiveconf users=$inputablename -hiveconf queue=$queue_name -f ./scripts/extract_address_table_to_text.hql

	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $current_table

	if [ $? -ne 0 ]; then
		echo "failed to execute the validation of ADDRESS table, exiting the script"
		exit -1
	fi
break  
 fi
done



#***************************************** end for adress table ******************************************************************

#***************************************** Start of zip table ********************************************************************
# extract BIN table data 
flat=-1
current_table=ZIP
adressfolder=Data/ZIP
extractlocation ="$bawdw_table_extract_location$adressfolder"

for key in "${!ary[@]}"; do
 if [[ $current_table -eq ${ary[$key]} ]]; then
 	if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hdfs dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
	# extract ADDRESS table data 
	hive -hiveconf users=$inputablename -hiveconf queue=$queue_name -f ./scripts/extract_zip_table_to_text.hql

	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $current_table

	if [ $? -ne 0 ]; then
		echo "failed to execute the validation of PERSON table, exiting the script"
		exit -1
	fi
    break  
 fi
done


#***************************************** end for zip table *********************************************************************



