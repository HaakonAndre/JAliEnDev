#!/bin/bash

cd `dirname $0`

CLASSPATH=.:../

for jar in `pwd`/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

export CLASSPATH

cd src

find . -name \*.java | xargs javac -O -g -d ../build_eclipse || exit 1

cd ../build_eclipse

jar cf ../alien.jar *

