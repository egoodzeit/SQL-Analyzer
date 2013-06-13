#!/bin/bash

# Use this script to build your code on the iLab machines

JAVA_HOME=/usr/local/java-archive/jdk1.7.0_11/
PATH=$JAVA_HOME/bin:/bin:/usr/bin

echo 
echo "running ant to compile code..."
ant
if [ $? != 0 ]; then
  exit 1
fi
