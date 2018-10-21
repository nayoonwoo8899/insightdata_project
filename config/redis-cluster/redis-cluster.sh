#!/bin/bash

PEG_ROOT=$PEGASUS_HOME

CLUSTER_NAME=redis-cluster

peg up redis-master.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} environment

peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
peg install ${CLUSTER_NAME} redis
