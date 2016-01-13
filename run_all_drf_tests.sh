#!/usr/bin/env bash
DRFV=">=2.3,<2.4 >=2.4,<3.0 >=3.0,<3.1  >=3.1,<3.2 >=3.2,<3.3 >=3.3,<3.4"
for REQ in $DRFV;
do
  pip install "djangorestframework$REQ" --use-mirrors -U
  python setup.py test
done
