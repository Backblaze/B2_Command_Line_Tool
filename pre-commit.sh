#!/bin/bash

function header 
{
    echo
    echo "#"
    echo "# $*"
    echo "#"
    echo
}

if yapf --version &> /dev/null
then
    echo "yapf is installed"
else
    echo "Please install yapf, then try again."
    exit 1
fi

header Unit Tests

if make test
then
    echo "Unit tests PASSED"
else
    echo "Unit tests FAILED"
    exit 1
fi

header Checking Formatting

SOURCE_FILES="b2/*.py test/*.py"

for src_file in $SOURCE_FILES
do
    echo "$src_file"
    if yapf "$src_file" > yapf.out
    then
        rm yapf.out
    else
        echo
        echo "Formatting updated:"
        echo
        diff "$src_file" yapf.out
        mv yapf.out "$src_file"
        sleep 5
    fi
done
chmod +x b2/b2.py

header Pyflakes

for d in b2 test
do
    if pyflakes $d
    then
        echo pyflakes passed on $d
    else
        echo pyflakes FAILED on %d
        exit 1
    fi
done

if [[ $# -ne 0 && "$1" == quick ]]
then
    header QUICK
    echo Skipping integration tests in quick mode.
    echo
    exit 0
fi

function run_integration_tests
{
    if time python test_b2_command_line.py $(head -n 1 ~/.b2_auth) $(tail -n 1 ~/.b2_auth)
    then
        echo "integration tests passed"
    else
        echo
        echo "integration tests FAILED"
        exit 1
    fi
}


if [[ -z "$PYTHON_VIRTUAL_ENVS" ]]
then
    run_integration_tests
else
    for virtual_env in $PYTHON_VIRTUAL_ENVS
    do
        header Integration tests in: $virtual_env
        source $virtual_env/bin/activate
        run_integration_tests
    done

fi

