#!/usr/bin/env bash

for REQ in $DRFV;
do
  pip install "djangorestframework$REQ" --use-mirrors -U
  python setup.py test
done
