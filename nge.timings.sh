#!/bin/sh

. ve/bin/activate
radical-stack

# for i in 1 2 3
for i in 1
do
  for n in 1 2 4 8 16 32 64 128 256 512 1024
  do
    echo "##################################### $n [$i] ##############################"
    ./examples/00_nge.py $n
    ./examples/00_rp.py  $n
    done
done
