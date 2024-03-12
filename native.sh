#!/usr/bin/env bash

./mvnw clean
./mvnw -DskipTests -Pnative native:compile
