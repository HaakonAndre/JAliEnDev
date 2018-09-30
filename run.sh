#!/bin/bash

if [[ -z "${JALIEN_HOME}" ]]; then
  ## find the location of jalien script
  SOURCE="${BASH_SOURCE[0]}"
  while [ -h "${SOURCE}" ]; do ## resolve $SOURCE until the file is no longer a symlink
    JALIEN_HOME="$( cd -P "$(dirname "${SOURCE}" )" && pwd )" ##"
    SOURCE="$(readlink "${SOURCE}")" ##"
    [[ "${SOURCE}" != /* ]] && SOURCE="${JALIEN_HOME}/${SOURCE}" ## if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
  done
  JALIEN_HOME="$(cd -P "$( dirname "${SOURCE}" )" && pwd)" ##"
  export JALIEN_HOME
fi

JAR_LIST_MAIN=$(find "${JALIEN_HOME}" -maxdepth 1 -name "*.jar" -printf "%p:" | sed 's/.$//')
JAR_LIST_LIB=$(find "${JALIEN_HOME}/lib/" -name "*.jar" -printf "%p:" | sed 's/.$//')
export CLASSPATH="${JAR_LIST_MAIN}:${JAR_LIST_LIB}"

JALIEN_OPTS_DEFAULT="-server -Xms4G -Xmx4G -XX:+UseG1GC -XX:+DisableExplicitGC -XX:+UseCompressedOops -XX:+AggressiveOpts \
-XX:+OptimizeStringConcat -XX:MaxTrivialSize=1K -XX:CompileThreshold=20000 -Duserid=$(id -u) -Dcom.sun.jndi.ldap.connect.pool=false -Djava.io.tmpdir=/tmp"

CMD="java ${JALIEN_OPTS_DEFAULT}"
eval "${CMD}" "$@"
