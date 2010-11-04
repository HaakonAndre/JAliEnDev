#!/bin/bash

CLASSPATH=bin:build_eclipse

for jar in lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

export CLASSPATH

java "$@"
