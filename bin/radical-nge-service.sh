#!/bin/sh

export RADICAL_PILOT_DBURL=mongodb://nge:nge@two.radical-project.org/nge

nohup radical-nge-service.py 2>&1 > radical-nge-service.log &

