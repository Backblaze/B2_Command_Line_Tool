#!/bin/bash -u

function header 
{
    echo
    echo "#"
    echo "# $*"
    echo "#"
    echo
}

SOURCE_FILES=b2/b2.py

header Checking Formatting

for src_file in $SOURCE_FILES
do
    echo $src_file
    if yapf $src_file > yapf.out
    then
        rm yapf.out
    else
        echo
        echo "Formatting updated:"
        echo
        diff $src_file yapf.out
        mv yapf.out $src_file
    fi
done
chmod +x b2/b2.py

header Pyflakes

for src_file in $SOURCE_FILES
do
    echo $src_file
    if pyflakes $src_file
    then
        echo "Pyflakes passed"
    else
        echo
        echo "Pyflakes FAILED"
        exit 1
    fi
done

header Tests

if time python test_b2_command_line.py ./b2/b2.py $(head -n 1 ~/.b2_auth) $(tail -n 1 ~/.b2_auth)
then
    echo "Tests passed"
else
    echo
    echo "Tests FAILED"
    exit 1
fi

