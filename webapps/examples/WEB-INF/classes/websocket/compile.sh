#!/bin/bash

TOMCAT_HOME=/home/vyurchen/workspace/apache-tomcat-8.0.24
CP=$(ls $TOMCAT_HOME/lib/*.jar | paste -s -d : -):$TOMCAT_HOME/bin/tomcat-juli.jar:$TOMCAT_HOME/lib2/json-simple-1.1.jar

javac -classpath $CP:..:$CP ~/workspace/apache-tomcat-8.0.24/webapps/examples/WEB-INF/classes/websocket/ExamplesConfig.java
