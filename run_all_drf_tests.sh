#!/usr/bin/env bash
for VER in $DRFV;
do
  TOP=`echo "$VER + 0.1" | bc`
  pip install "djangorestframework>=$VER,<$TOP" --use-mirrors -U
  python setup.py test
done
