#!/bin/bash -u

function header 
{
    echo
    echo "#"
    echo "# $*"
    echo "#"
    echo
}

header Checking Formatting

if yapf b2 > b2.yapf.out
then
    echo "Formatting is good."
    rm b2.yapf.out
else
    echo "Formatting updated:"
    echo
    diff b2 b2.yapf.out
    mv b2.yapf.out b2
    chmod +x b2
fi

header Pyflakes

if pyflakes b2
then
    echo "Pyflakes passed"
else
    echo
    echo "Pyflakes FAILED"
    exit 1
fi    

header Tests

if time python test_b2_command_line.py ./b2 $(head -n 1 ~/.b2_auth) $(tail -n 1 ~/.b2_auth)
then
    echo "Tests passed"
else
    echo
    echo "Tests FAILED"
    exit 1
fi

