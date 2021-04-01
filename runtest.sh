#!/bin/bash

# fail if smth fails
# the whole env will be running if test suite fails so you can debug
set -e

# for debugging this script, b/c I sometimes get
# unable to prepare context: The Dockerfile (Dockerfile.tests) must be within the build context (.)
set -x

# test coverage threshold
COVERAGE_THRESHOLD=15

export TERM=xterm
TERM=${TERM:-xterm}

# set up terminal colors
NORMAL=$(tput sgr0)
RED=$(tput bold && tput setaf 1)
GREEN=$(tput bold && tput setaf 2)
YELLOW=$(tput bold && tput setaf 3)

printf "%sCreate Virtualenv for Python deps ..." "${NORMAL}"

check_python_version() {
    python3 tools/check_python_version.py 3 6
}

function prepare_venv() {
    set +e
    VIRTUALENV="$(which virtualenv)"
    if [ $? -eq 1 ]; then
        echo "Trying to find virualenv-3"
        # python36 which is in CentOS does not have virtualenv binary
        VIRTUALENV="$(which virtualenv-3)"
    fi
    if [ $? -eq 1 ]; then
        echo "Virtualenv binary can't be found, using venv module instead"
        # still don't have virtual environment -> use python3 directly
        python3 -m venv venv_test && source venv_test/bin/activate
    else
        ${VIRTUALENV} -p python3 venv_test && source venv_test/bin/activate
    fi
    if [ $? -ne 0 ]
    then
        printf "%sPython virtual environment can't be initialized%s" "${RED}" "${NORMAL}"
        exit 1
    fi
    printf "%sPython virtual environment initialized%s\n" "${YELLOW}" "${NORMAL}"
    set -e
}

check_python_version

[ "$NOVENV" == "1" ] || prepare_venv || exit 1

here=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

export PYTHONPATH=${here}/

# Need to use 19.2.3 version only as some of packages are deprecated in latest PIP version.
pip3 install pip==19.2.3
pip3 install -r requirements.txt
pip3 install -r requirements_test.txt


echo "*****************************************"
echo "*** Cyclomatic complexity measurement ***"
echo "*****************************************"
radon cc -s -a -e "venv/*,venv_test/*" .

echo "*****************************************"
echo "*** Maintainability Index measurement ***"
echo "*****************************************"
radon cc -s -a -e "venv/*,venv_test/*" .

echo "*****************************************"
echo "*** Unit tests ***"
echo "*****************************************"

echo "Starting test suite"
DISABLE_AUTHENTICATION=1 PYTHONDONTWRITEBYTECODE=1 python "$(which pytest)" --cov=./src/ --cov-report term-missing --cov-fail-under=$COVERAGE_THRESHOLD -vv ./tests/
printf "%stests passed%s\n\n" "${GREEN}" "${NORMAL}"

`which codecov` --token=5c0f0ca6-c3aa-407f-9b61-07830d9325e5

rm -rf venv_test
