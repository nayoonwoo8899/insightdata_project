#!/bin/bash

PEG_ROOT=$PEGASUS_HOME

CLUSTER_NAME=spark-cluster

peg up spark-master.yml &
peg up spark-workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment

peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} spark

peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
