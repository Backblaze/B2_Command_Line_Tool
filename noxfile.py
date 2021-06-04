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
import platform
import subprocess
from glob import glob

import nox

CI = os.environ.get('CI') is not None
INSTALL_SDK_FROM = os.environ.get('INSTALL_SDK_FROM')
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')

PYTHON_VERSIONS = ['3.5', '3.6', '3.7', '3.8', '3.9'
                  ] if NOX_PYTHONS is None else NOX_PYTHONS.split(',')
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

PY_PATHS = ['b2', 'test', 'noxfile.py', 'setup.py']

REQUIREMENTS_FORMAT = ['yapf==0.27']
REQUIREMENTS_LINT = ['yapf==0.27', 'pyflakes==2.2.0', 'pytest==6.1.1', 'liccheck==0.4.7']
REQUIREMENTS_TEST = ['pytest==6.1.1', 'pytest-cov==2.10.1']
REQUIREMENTS_BUILD = ['setuptools>=20.2']

OSX_BUNDLE_IDENTIFIER = 'com.backblaze.b2'
OSX_BUNDLE_ENTITLEMENTS = 'contrib/macos/entitlements.plist'

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'lint',
    'test',
]

# In CI, use Python interpreter provided by GitHub Actions
if CI:
    nox.options.force_venv_backend = 'none'


def install_myself(session, extras=None):
    """Install from the source."""
    # In CI, install B2 SDK from the master branch

    if CI and not INSTALL_SDK_FROM:
        session.install('git+git://github.com/Backblaze/b2-sdk-python#egg=b2sdk')

    arg = '.'
    if extras:
        arg += '[%s]' % ','.join(extras)

    session.install('-e', arg)
    if INSTALL_SDK_FROM:
        cwd = os.getcwd()
        os.chdir(INSTALL_SDK_FROM)
        session.run('pip', 'uninstall', 'b2sdk', '-y')
        session.run('python', 'setup.py', 'develop')
        os.chdir(cwd)


@nox.session(name='format', python=PYTHON_DEFAULT_VERSION)
def format_(session):
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
    install_myself(session)
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
    session.run('liccheck', '-s', 'setup.cfg')


@nox.session(python=PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""
    install_myself(session)
    session.install(*REQUIREMENTS_TEST)
    session.run(
        'pytest', '--cov=b2', '--cov-branch', '--cov-report=xml', '--doctest-modules',
        *session.posargs, 'test/unit'
    )
    if not session.posargs:
        session.notify('cover')


@nox.session(python=PYTHON_VERSIONS)
def integration(session):
    """Run integration tests."""
    install_myself(session)
    session.install(*REQUIREMENTS_TEST)
    session.run('pytest', '-s', *session.posargs, 'test/integration')


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    """Run all tests."""
    if session.python:
        session.notify('unit-{}'.format(session.python))
        session.notify('integration-{}'.format(session.python))
    else:
        session.notify('unit')
        session.notify('integration')


@nox.session
def cover(session):
    """Perform coverage analysis."""
    session.install('coverage')
    session.run('coverage', 'report', '--fail-under=75', '--show-missing', '--skip-covered')
    session.run('coverage', 'erase')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build(session):
    """Build the distribution."""
    # TODO: consider using wheel as well
    session.install(*REQUIREMENTS_BUILD)
    session.run('python', 'setup.py', 'check', '--metadata', '--strict')
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True)
    session.run('python', 'setup.py', 'sdist', *session.posargs)

    # Set outputs for GitHub Actions
    if CI:
        asset_path = glob('dist/*')[0]
        print('::set-output name=asset_path::', asset_path, sep='')

        asset_name = os.path.basename(asset_path)
        print('::set-output name=asset_name::', asset_name, sep='')

        version = os.environ['GITHUB_REF'].replace('refs/tags/v', '')
        print('::set-output name=version::', version, sep='')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def bundle(session):
    """Bundle the distribution."""
    session.install('pyinstaller')
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True)
    install_myself(session)

    system = platform.system().lower()

    if system == 'darwin':
        session.posargs.extend(['--osx-bundle-identifier', OSX_BUNDLE_IDENTIFIER])

    session.run('pyinstaller', '--onefile', *session.posargs, 'b2.spec')

    # Set outputs for GitHub Actions
    if CI:
        asset_path = glob('dist/*')[0]
        print('::set-output name=asset_path::', asset_path, sep='')

        name, ext = os.path.splitext(os.path.basename(asset_path))
        asset_name = '{}-{}{}'.format(name, system, ext)
        print('::set-output name=asset_name::', asset_name, sep='')


@nox.session(python=False)
def sign(session):
    """Sign the bundled distribution (OSX only)."""
    system = platform.system().lower()

    if system != 'darwin':
        session.skip('signing process is for OSX only')

    session.run('security', 'find-identity', external=True)
    session.run(
        'codesign',
        '--deep',
        '--force',
        '--verbose',
        '--timestamp',
        '--identifier',
        OSX_BUNDLE_IDENTIFIER,
        '--entitlements',
        OSX_BUNDLE_ENTITLEMENTS,
        '--options',
        'runtime',
        *session.posargs,
        'dist/b2',
        external=True
    )
    session.run('codesign', '--verify', '--verbose', 'dist/b2', external=True)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def doc(session):
    """Build the documentation."""
    install_myself(session, extras=['doc'])
    session.cd('doc')
    sphinx_args = ['-b', 'html', '-T', '-W', 'source', 'build/html']
    session.run('rm', '-rf', 'build', external=True)

    if not session.interactive:
        session.run('sphinx-build', *sphinx_args)
        session.notify('doc_cover')
    else:
        sphinx_args[-2:-2] = [
            '-E', '--open-browser', '--watch', '../b2', '--ignore', '*.pyc', '--ignore', '*~'
        ]
        session.run('sphinx-autobuild', *sphinx_args)


@nox.session
def doc_cover(session):
    """Perform coverage analysis for the documentation."""
    install_myself(session, extras=['doc'])
    session.cd('doc')
    sphinx_args = ['-b', 'coverage', '-T', '-W', 'source', 'build/coverage']
    report_file = 'build/coverage/python.txt'
    session.run('sphinx-build', *sphinx_args)
    session.run('cat', report_file, external=True)

    with open('build/coverage/python.txt') as fd:
        # If there is no undocumented files, the report should have only 2 lines (header)
        if sum(1 for _ in fd) != 2:
            session.error('sphinx coverage has failed')
