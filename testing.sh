#!/bin/bash

DIR=".dprofiler"
# check if .dprofiler exist to avoid misleading errors.
if [ -d $DIR ]; then
   echo ".dprofiler exist in current working directory"
   rm -rf $DIR
   pytest -v
else
   echo "starting unit testing"
   pytest -v
fi
# to insure that .dprofiler doesn't exist in the next testing trial
rm -rf $DIR