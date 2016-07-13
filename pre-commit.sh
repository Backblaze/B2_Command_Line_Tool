#!/bin/bash -u

base_remote="${1:-origin}"
base_branch="${2:-master}"
base_remote_branch="${3:-master}"

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

if ./run-unit-tests.sh
then
    echo "Unit tests PASSED"
else
    echo "Unit tests FAILED"
    exit 1
fi

header Checking Formatting

if [ "$(<.git/refs/heads/${base_branch})" != "$(<.git/refs/remotes/${base_remote}/${base_remote_branch})" ]; then
    echo """running yapf in full mode, because an assumption that master and origin/master are the same, is broken. To fix it, do this:
git checkout master
git pull --ff-only

then checkout your topic branch and run $0.
If the base branch on github is not called 'origin', invoke as $0 proper_origin_remote_name. Then your remote needs to be synched with your master too.
"""
    yapf --in-place --recursive .
else
    echo 'running yapf in incremental mode'
    head=`mktemp`
    master=`mktemp`
    git rev-list --first-parent HEAD > "$head"  # list of commits being a history of HEAD branch, but without commits merged from master after forking
    git rev-list origin/master > "$master"  # list of all commits on history of master

    changed_files=`git diff --name-only "$(git rev-parse --abbrev-ref HEAD)..${base_remote}/${base_remote_branch}"`
    dirty_files=`git ls-files -m`
    files_to_check="$((echo "$changed_files"; echo "$dirty_files") | grep '\.py$' | sort -u)"
    if [ -z "$files_to_check" ]; then
        echo 'nothing to run yapf on after all'
    else
        echo -n 'running yapf... '

        echo "$files_to_check" | (while read file
        do
            yapf --in-place "$file" &
        done
        wait
        )

        echo 'done'
    fi
fi

header Pyflakes

for d in b2 test *.py
do
    if pyflakes "$d"
    then
        echo "pyflakes passed on $d"
    else
        echo "pyflakes FAILED on $d"
        exit 1
    fi
done

header test_raw_api

if [[ -n "$TEST_ACCOUNT_ID" && -n "$TEST_APPLICATION_KEY" ]]
then
    python -m b2.__main__ test_raw_api
else
    echo "Skipping because TEST_ACCOUNT_ID and TEST_APPLICATION_KEY are not set"
fi

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

# not sure why this is needed to make virtualenv activation work
PS1=""

if [[ -z "$PYTHON_VIRTUAL_ENVS" ]]
then
    run_integration_tests
else
    for virtual_env in $PYTHON_VIRTUAL_ENVS
    do
        header "Integration tests in: $virtual_env"
        source "$virtual_env/bin/activate"
        run_integration_tests
    done

fi

