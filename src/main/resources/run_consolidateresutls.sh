#!/bin/sh

# export config


# Script used to read Property File
file="./config/dwconfig.bin.properties"
prop_value=""
key="dwvalidation.result.location"
getProperty ${key}

echo "Key = ${key} ; Value = " ${prop_value}



getProperty()
{
        prop_key=$1
        prop_value=`cat ${FILE_NAME} | grep ${prop_key} | cut -d'=' -f2`
}


