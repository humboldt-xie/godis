#!/bin/bash
#===============================================================================
#
#          FILE:  run.sh
# 
#         USAGE:  ./run.sh 
# 
#   DESCRIPTION:  
# 
#       OPTIONS:  ---
#  REQUIREMENTS:  ---
#          BUGS:  ---
#         NOTES:  ---
#        AUTHOR:  humboldt (), humboldt-xie@163.com
#       COMPANY:  humboldt
#       VERSION:  1.0
#       CREATED:  05/21/2014 07:39:43 PM CST
#      REVISION:  ---
#===============================================================================

export set LD_LIBRARY_PATH="../:$LIBRARY_PATH";

#ldd ./a.out
make
./a.out
