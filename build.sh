#!/bin/bash

echo -n 'input the act(clean build idea DistTar):'; read act;

if [[ -z $act ]]; then
	./gradlew -q clean;
else
	./gradlew -q -x test $act;
fi
