#!/bin/bash

export PATH="$PATH:$HOME/alien/bin:$HOME/alien/api/bin"
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$HOME/alien/lib:$HOME/alien/api/lib"

if [ -f alien-cs.jar ]; then
    CLASSPATH="alien-cs.jar"
elif [ -f alien-users.jar ]; then
    CLASSPATH="alien-users.jar"
else
    CLASSPATH="alien.jar"

    for DEP in lib/*.jar; do
	CLASSPATH="$CLASSPATH:$DEP"
    done
fi

export CLASSPATH

java \
	-server \
	-Xms4G -Xmx4G \
	-XX:+UseG1GC \
	-XX:+DisableExplicitGC \
	-XX:+UseCompressedOops \
	-XX:+AggressiveOpts \
	-XX:+OptimizeStringConcat \
	-XX:MaxTrivialSize=1K \
	-XX:CompileThreshold=20000 \
	-Duserid=$(id -u) \
	-Dcom.sun.jndi.ldap.connect.pool=false \
	"$@"
