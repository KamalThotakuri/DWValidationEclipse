#!/bin/bash

# command to invoke 
# ./run_queries.sh ./lib/csv-serde-1.1.2-0.11.0-all.jar ./lib/ngrams-1.0.jar ./out ./tweets ./stop_words/ edureka

echo $# arguments passed
if [ $# -ne 3 ]; then 
    echo "Illegal number of parameters, this script requires 6 arguments <hive_csv_serde_jar> <udf_jar> <out_data_dir> <tweets_data_dir> <stop_words_file> <queue_name>"
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
bin_table=BIN
binfolder=Data/BIN
extractlocation="$bawdw_table_extract_location$binfolder"
echo "****$bin_table"
echo "$extractlocation"
for key in "${!ary[@]}"; do
echo "BINNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN$bin_table"
echo " Arreyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy**${ary[$key]} "
 if [[ $bin_table == ${ary[$key]} ]]; then
     echo "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiinside Bin if "
	if [ -d "$extractlocation" ];then
	 hadoop dfs -rmr $extractlocation
	fi
  
	 
	if [ $? -ne 0 ]; then
			echo "1failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hadoop dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "1failed to create $extractlocation, exiting the script"
			exit -1
	fi
     hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f ./scripts/extract_business_table_to_text.hql
	if [ $? -ne 0 ]; then
			echo "1failed to extract BIN table data, exiting the script"
			exit -1
	fi
		# MR job for  BIN table data validation
		hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $bin_table

		if [ $? -ne 0 ]; then
			echo "1failed to execute the validation of BIN table, exiting the script"
			exit -1
		fi
    break  
 fi
done

#***************************************** end for BIN table *****************************************************************


#***************************************** Start of PERSON table *************************************************************

flat=-1
pin_table=PIN
pinfolder=Data/PIN
extractlocation="$bawdw_table_extract_location$pinfolder"
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@$pin_table"
for key in "${!ary[@]}"; do
 if [[ $pin_table -eq ${ary[$key]} ]]; then
    echo "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiinside pin if "
    if [ -d "$extractlocation" ];then
	 hadoop dfs -rmr $extractlocation
	fi
 	if [ $? -ne 0 ]; then
			echo "2failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	hadoop dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "2failed to create $extractlocation, exiting the script"
			exit -1
	fi
	# extract PERSON table data 
	hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f ./scripts/extract_person_table_to_text.hql
	if [ $? -ne 0 ]; then
		echo "2failed to extract PIN table data, exiting the script"
		exit -1
	fi
	echo "$pin_table"
	# MR job for  PIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $pin_table

	if [ $? -ne 0 ]; then
		echo "2failed to execute the validation of PERSON table, exiting the script"
		exit -1
	fi
    break  
 fi
done
#***************************************** end for PERSON table ****************************************************************


#***************************************** Start of PERSON_AT_BUSINESS table ****************************************************
flat=-1
pbl_table=PBL
pblfolder=Data/PBL
extractlocation="$bawdw_table_extract_location$pblfolder"
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@$extractlocation"
for key in "${!ary[@]}"; do
 if [[ $pbl_table -eq ${ary[$key]} ]]; then
    if [ -d "$extractlocation" ];then
	 hadoop dfs -rmr $extractlocation
	fi
 	 if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hadoop dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
	# extract PBL table data 
	hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f ./scripts/extract_person_at_business_table_to_text.hql

	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $pbl_table

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
address_table=ADDRESS
adressfolder=Data/ADDRESS
extractlocation="$bawdw_table_extract_location$adressfolder"
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@$extractlocation"
for key in "${!ary[@]}"; do
 if [[ $current_table -eq ${ary[$key]} ]]; then
    if [ -d "$extractlocation" ];then
	 hadoop dfs -rmr $extractlocation
	fi
	if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hadoop dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
# extract ADDRESS table data 
	hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f ./scripts/extract_address_table_to_text.hql

	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $address_table

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
zip_table=ZIP9
adressfolder=Data/ZIP9
extractlocation="$bawdw_table_extract_location$adressfolder"
echo "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@$extractlocation"
for key in "${!ary[@]}"; do
 if [[ $zip_table -eq ${ary[$key]} ]]; then
    if [ -d "$extractlocation" ];then
	 hadoop dfs -rmr $extractlocation
	fi
 	if [ $? -ne 0 ]; then
			echo "failed to remove $extractlocation, exiting the script"
			exit -1
	fi
	 hadoop dfs -mkdir $extractlocation
	if [ $? -ne 0 ]; then
			echo "failed to create $extractlocation, exiting the script"
			exit -1
	fi
	# extract ADDRESS table data 
	hive -hiveconf out=$extractlocation -hiveconf queue=$queue_name -f ./scripts/extract_zip_table_to_text.hql

	if [ $? -ne 0 ]; then
		echo "failed to extract BIN table data, exiting the script"
		exit -1
	fi
	# MR job for  BIN table data validation
	hadoop jar ./lib/dataload_validation-1.0.0.jar com.acxiom.pmp.mr.dataloadvalidation.ValidatorRunner $zip_table

	if [ $? -ne 0 ]; then
		echo "failed to execute the validation of PERSON table, exiting the script"
		exit -1
	fi
    break  
 fi
done


#***************************************** end for zip table *********************************************************************



