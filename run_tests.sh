#!/bin/bash

PYTHON_VERSION=$(python --version 2>&1)

if [[ -z "$DJANGO" ]]
then
  DJANGO=`pip freeze|grep Django==`
fi

if [[ "$PYTHON_VERSION" > "Python 3" ]]
then
  if [[ "$DJANGO" < "1.5" ]]
  then
    echo "Cannot run tests with $DJANGO on $PYTHON_VERSION"
    exit
  fi
fi

python setup.py test
