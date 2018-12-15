#!/usr/bin/env bash

git pull --rebase origin master
mvn clean install
cp -ruf ./* $HOME/subgraph-mining/