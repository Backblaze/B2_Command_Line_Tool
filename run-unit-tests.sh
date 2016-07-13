#!/bin/bash -e
if [ -z "${PYTHON_VIRTUAL_ENVS}" ]; then
    python setup.py nosetests
else
    for virtual_env in ${PYTHON_VIRTUAL_ENVS}
    do
        echo Activating ${virtual_env}
        source ${virtual_env}/bin/activate
        python setup.py nosetests
    done
fi

echo
echo '#################'
echo '# unit tests OK #'
echo '#################'
