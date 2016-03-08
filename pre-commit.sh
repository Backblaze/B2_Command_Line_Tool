#!/bin/bash -u

function header 
{
    echo
    echo "#"
    echo "# $*"
    echo "#"
    echo
}

SOURCE_FILES="b2/*.py test/*.py"

if yapf --version &> /dev/null
then
    echo "yapf is installed"
else
    echo "Please install yapf, then try again."
    exit 1
fi

header Checking Formatting

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

header Unit Tests

if PYTHONPATH=`pwd` nosetests -w test
then
    echo "Unit tests passed."
else
    exit 1
fi

if [[ $1 != quick ]]
then
    header Integration Tests

    if time python test_b2_command_line.py ./b2/b2.py $(head -n 1 ~/.b2_auth) $(tail -n 1 ~/.b2_auth)
    then
        echo "python tests passed"
    else
        echo
        echo "python tests FAILED"
        exit 1
    fi

    if time python3 test_b2_command_line.py ./b2/b2.py $(head -n 1 ~/.b2_auth) $(tail -n 1 ~/.b2_auth)
    then
        echo "python3 tests passed"
    else
        echo
        echo "python3 tests FAILED"
        exit 1
    fi
fi

