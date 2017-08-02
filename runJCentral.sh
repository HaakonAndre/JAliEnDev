#!/bin/bash

cd `dirname $0`

pkill -f alien.JCentral

if [ -z "$JALIEN_HOME" ]; then
    JALIEN_HOME=`pwd`
fi

CONFIG_DIR=${JALIEN_CONFIG_DIR:-${HOME}/.j/config}

# This flag helps debugging SSL connection errors
#    -Djavax.net.debug=all \

export JAVA_HOME=${JAVA_HOME:-/opt/java}

export PATH=$JAVA_HOME/bin:$PATH

./run.sh \
    -XX:CompileThreshold=5 \
    -DAliEnConfig=${CONFIG_DIR} \
    -Dsun.security.ssl.allowUnsafeRenegotiation=true \
    alien.JCentral \
&>jcentral.log </dev/null &

