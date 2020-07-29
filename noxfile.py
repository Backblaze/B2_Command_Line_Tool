######################################################################
#
# File: noxfile.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import subprocess

import nox

CI = os.environ.get('CI') is not None
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')

PYTHON_VERSIONS = ['3.5', '3.6', '3.7', '3.8'] if NOX_PYTHONS is None else NOX_PYTHONS.split(',')
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

PY_PATHS = ['b2', 'test', 'noxfile.py', 'setup.py']

# TODO: remove nose and pyflakes
REQUIREMENTS_FORMAT = ['docformatter==1.3.1', 'isort==5.1.1', 'yapf==0.27']
REQUIREMENTS_LINT = [*REQUIREMENTS_FORMAT, 'pyflakes', 'flake8==3.8.3', 'pytest==5.4.3']
REQUIREMENTS_TEST = ['nose==1.3.7', 'pytest==5.4.3', 'pytest-cov==2.10.0']
REQUIREMENTS_BUILD = ['liccheck==0.4.7', 'setuptools>=20.2']
REQUIREMENTS_DOC = [
    'sphinx', 'sphinx-autobuild', 'sphinx_rtd_theme', 'sphinx-argparse', 'sphinxcontrib-plantuml',
    'sadisplay'
]

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'lint',
    'test',
]

# In CI, use Python interpreter provided by GitHub Actions
if CI:
    nox.options.force_venv_backend = 'none'


# noinspection PyShadowingBuiltins
@nox.session(python=PYTHON_DEFAULT_VERSION)
def format(session):
    """Format the code."""
    session.install(*REQUIREMENTS_FORMAT)
    # TODO: incremental mode for yapf
    session.run('yapf', '--in-place', '--parallel', '--recursive', *PY_PATHS)
    # TODO: uncomment if we want to use isort and docformatter
    # session.run('isort', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--in-place',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )


@nox.session(python=PYTHON_DEFAULT_VERSION)
def lint(session):
    """Run linters."""
    session.install(*REQUIREMENTS_LINT)
    session.run('yapf', '--diff', '--parallel', '--recursive', *PY_PATHS)
    # TODO: uncomment if we want to use isort and docformatter
    # session.run('isort', '--check', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--check',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )

    # TODO: use flake8 instead of pyflakes
    session.log('pyflakes b2')
    output = subprocess.run('pyflakes b2', shell=True, check=False,
                            stdout=subprocess.PIPE).stdout.decode().strip()
    excludes = ['__init__.py']
    output = [l for l in output.splitlines() if all(x not in l for x in excludes)]
    if output:
        print('\n'.join(output))
        session.error('pyflakes has failed')
    # session.run('flake8', *PY_PATHS)
    session.run('pytest', 'test/static')


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    """Run test suite."""
    session.install('-e', '.', *REQUIREMENTS_TEST)

    if session.posargs:
        # Run given test suite
        session.run('pytest', '--cov=b2', '--cov-branch', '--cov-report=xml', *session.posargs)
    else:
        # By default, run all suites and show the coverage
        session.run(
            'pytest', '--cov=b2', '--cov-branch', '--cov-report=xml', '--doctest-modules',
            'test/unit'
        )
        session.run('pytest', '-s', 'test/integration', env=os.environ)


@nox.session
def cover(session):
    """Perform coverage analysis."""
    session.install('coverage')
    session.run('coverage', 'report', '--fail-under=75', '--show-missing')
    session.run('coverage', 'erase')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build(session):
    """Build the distribution."""
    # TODO: consider using wheel as well
    session.install('-e', '.', *REQUIREMENTS_BUILD)
    session.run('liccheck', '-s', 'setup.cfg')
    session.run('python', 'setup.py', 'check', '--metadata', '--strict')
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True)
    session.run('python', 'setup.py', 'sdist', *session.posargs)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def deploy(session):
    """Deploy the distribution to the PyPi."""
    session.install('twine')
    session.run('twine', 'upload', 'dist/*')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def doc(session):
    """Build the documentation."""
    session.install('-e', '.', *REQUIREMENTS_DOC)
    session.cd('doc')
    sphinx_args = ['-b', 'html', '-T', '-W', 'source', 'build/html']
    session.run('rm', '-rf', 'build', external=True)

    if not session.interactive:
        session.run('sphinx-build', *sphinx_args)
        session.notify('doc_cover')
    else:
        sphinx_args[-2:-2] = ['--open-browser', '-z', '../b2', '-i', '*.pyc', '-i', '*~']
        session.run('sphinx-autobuild', *sphinx_args)


@nox.session
def doc_cover(session):
    """Perform coverage analysis for the documentation."""
    session.install('-e', '.', *REQUIREMENTS_DOC)
    session.cd('doc')
    sphinx_args = ['-b', 'coverage', '-T', '-W', 'source', 'build/coverage']
    report_file = 'build/coverage/python.txt'
    session.run('sphinx-build', *sphinx_args)
    session.run('cat', report_file, external=True)

    with open('build/coverage/python.txt') as fd:
        # If there is no undocumented files, the report should have only 2 lines (header)
        if sum(1 for _ in fd) != 2:
            session.error('sphinx coverage has failed')
