#!/bin/sh

today=`date +%Y%m%d`
sed -e "s/%today/$today/" configure.ac.barebone > configure.ac
./autogen.sh
