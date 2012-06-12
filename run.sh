#!/bin/bash

export PATH="$PATH:$HOME/alien/bin:$HOME/alien/api/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$HOME/alien/lib:$HOME/alien/api/lib"

CLASSPATH=bin:build_eclipse

for jar in lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

export CLASSPATH

java \
	-Duserid=$(id -u) \
	-Dcom.sun.jndi.ldap.connect.pool=false \
	"$@"
