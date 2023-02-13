######################################################################
#
# File: noxfile.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import hashlib
import os
import pathlib
import platform
import subprocess
from glob import glob
from typing import List

import nox
import pkg_resources

CI = os.environ.get('CI') is not None
CD = CI and (os.environ.get('CD') is not None)
INSTALL_SDK_FROM = os.environ.get('INSTALL_SDK_FROM')
NO_STATICX = os.environ.get('NO_STATICX') is not None
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')

PYTHON_VERSIONS = [
    '3.7',
    '3.8',
    '3.9',
    '3.10',
    '3.11',
] if NOX_PYTHONS is None else NOX_PYTHONS.split(',')
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

PY_PATHS = ['b2', 'test', 'noxfile.py', 'setup.py']

SYSTEM = platform.system().lower()

REQUIREMENTS_FORMAT = ['yapf==0.27']
REQUIREMENTS_LINT = ['yapf==0.27', 'pyflakes==2.4.0', 'pytest==6.2.5', 'liccheck==0.6.2']
REQUIREMENTS_TEST = [
    "pytest==6.2.5",
    "pytest-cov==3.0.0",
    'pytest-xdist==2.5.0',
    'backoff==2.1.2',
    'more_itertools==8.13.0',
]
REQUIREMENTS_BUILD = ['setuptools>=20.2']
REQUIREMENTS_BUNDLE = [
    'pyinstaller==5.6.2',
    "patchelf-wrapper==1.2.0;platform_system=='Linux'",
    "staticx==0.13.5;platform_system=='Linux'",
]

WINDOWS_TIMESTAMP_SERVER = 'http://timestamp.digicert.com'
WINDOWS_SIGNTOOL_PATH = 'C:/Program Files (x86)/Windows Kits/10/bin/10.0.17763.0/x86/signtool.exe'

nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = [
    'lint',
    'test',
]

run_kwargs = {}

# In CI, use Python interpreter provided by GitHub Actions
if CI:
    nox.options.force_venv_backend = 'none'

    # Inside the CI we need to silence most of the outputs to be able to use GITHUB_OUTPUT properly.
    # Nox passes `stderr` and `stdout` directly to subprocess.Popen.
    run_kwargs = dict(
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )


def install_myself(session, extras=None):
    """Install from the source."""

    arg = '.'
    if extras:
        arg += '[%s]' % ','.join(extras)

    session.run('pip', 'install', '-e', arg, **run_kwargs)

    if INSTALL_SDK_FROM:
        cwd = os.getcwd()
        os.chdir(INSTALL_SDK_FROM)
        session.run('pip', 'uninstall', 'b2sdk', '-y')
        session.run('python', 'setup.py', 'develop')
        os.chdir(cwd)
    elif CI and not CD:
        # In CI, install B2 SDK from the master branch
        session.run(
            'pip', 'install', 'git+https://github.com/Backblaze/b2-sdk-python.git#egg=b2sdk',
            **run_kwargs
        )


@nox.session(name='format', python=PYTHON_DEFAULT_VERSION)
def format_(session):
    """Format the code."""
    session.run('pip', 'install', *REQUIREMENTS_FORMAT)
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
    session.run('pip', 'install', *REQUIREMENTS_LINT)

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

    # Before checking licenses, create an updated requirements.txt file, which accepts any b2sdk version.  This way
    # the tool will still work if the SDK was installed from the master branch or a different directory.
    updated_requirements = os.path.join(session.create_tmp(), 'requirements.txt')
    with open('requirements.txt', 'r') as orig_req_file, \
            open(updated_requirements, 'w') as updated_req_file:
        requirements = pkg_resources.parse_requirements(orig_req_file)
        for requirement in requirements:
            if requirement.project_name == "b2sdk":
                updated_req_file.write("b2sdk\n")
            else:
                updated_req_file.write(f"{requirement}\n")

    session.run('liccheck', '-s', 'setup.cfg', '-r', updated_requirements)


@nox.session(python=PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""
    install_myself(session, ['license'])
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    session.run(
        'pytest',
        '-n',
        'auto',
        '--cov=b2',
        '--cov-branch',
        '--cov-report=xml',
        '--doctest-modules',
        *session.posargs,
        'test/unit',
    )
    if not session.posargs:
        session.notify('cover')


@nox.session(python=PYTHON_VERSIONS)
def integration(session):
    """Run integration tests."""
    install_myself(session, ['license'])
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    session.run(
        'pytest',
        '-s',
        '-n',
        'auto',
        '-W',
        'ignore::DeprecationWarning:rst2ansi.visitor:',
        *session.posargs,
        'test/integration',
    )


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    """Run all tests."""
    if session.python:
        session.notify('unit-{}'.format(session.python))
        session.notify('integration-{}'.format(session.python))
    else:
        session.notify('unit')
        session.notify('integration')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def cleanup_buckets(session):
    """Remove buckets from previous test runs."""
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    session.run('pytest', '-s', '-x', *session.posargs, 'test/integration/cleanup_buckets.py')


@nox.session
def cover(session):
    """Perform coverage analysis."""
    session.run('pip', 'install', 'coverage')
    session.run('coverage', 'report', '--fail-under=75', '--show-missing', '--skip-covered')
    session.run('coverage', 'erase')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build(session):
    """Build the distribution."""
    # TODO: consider using wheel as well
    session.run('pip', 'install', *REQUIREMENTS_BUILD, **run_kwargs)
    session.run('nox', '-s', 'dump_license', '-fb', 'venv', **run_kwargs)
    session.run('python', 'setup.py', 'check', '--metadata', '--strict', **run_kwargs)
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True, **run_kwargs)
    session.run('python', 'setup.py', 'sdist', *session.posargs, **run_kwargs)

    # Set outputs for GitHub Actions
    if CI:
        # Path have to be specified with unix style slashes even for windows,
        # otherwise glob won't find files on windows in action-gh-release.
        print('asset_path=dist/*')

        version = os.environ['GITHUB_REF'].replace('refs/tags/v', '')
        print(f'version={version}')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def dump_license(session: nox.Session):
    install_myself(session, ['license'])
    session.run('b2', 'license', '--dump', '--with-packages')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def bundle(session: nox.Session):
    """Bundle the distribution."""
    session.run('pip', 'install', *REQUIREMENTS_BUNDLE, **run_kwargs)
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True, **run_kwargs)
    install_myself(session, ['license'])
    session.run('b2', 'license', '--dump', '--with-packages', **run_kwargs)

    session.run('pyinstaller', *session.posargs, 'b2.spec', **run_kwargs)

    if SYSTEM == 'linux' and not NO_STATICX:
        session.run(
            'staticx', '--no-compress', '--strip', '--loglevel', 'INFO', 'dist/b2',
            'dist/b2-static', **run_kwargs
        )
        session.run('mv', '-f', 'dist/b2-static', 'dist/b2', external=True, **run_kwargs)

    # Set outputs for GitHub Actions
    if CI:
        # Path have to be specified with unix style slashes even for windows,
        # otherwise glob won't find files on windows in action-gh-release.
        print('asset_path=dist/*')

        executable = str(next(pathlib.Path('dist').glob('*')))
        print(f'sut_path={executable}')


@nox.session(python=False)
def sign(session):
    """Sign the bundled distribution (macOS and Windows only)."""

    def sign_windows(cert_file, cert_password):
        session.run('certutil', '-f', '-p', cert_password, '-importpfx', cert_file, **run_kwargs)
        session.run(
            WINDOWS_SIGNTOOL_PATH,
            'sign',
            '/f',
            cert_file,
            '/p',
            cert_password,
            '/tr',
            WINDOWS_TIMESTAMP_SERVER,
            '/td',
            'sha256',
            '/fd',
            'sha256',
            'dist/b2.exe',
            external=True,
            **run_kwargs
        )
        session.run(
            WINDOWS_SIGNTOOL_PATH,
            'verify',
            '/pa',
            '/all',
            'dist/b2.exe',
            external=True,
            **run_kwargs
        )

    if SYSTEM == 'windows':
        try:
            certificate_file, certificate_password = session.posargs
        except ValueError:
            session.error('pass the certificate file and the password as positional arguments')
            return

        sign_windows(certificate_file, certificate_password)
    elif SYSTEM == 'linux':
        session.log('signing is not supported for Linux')
    else:
        session.error('unrecognized platform: {}'.format(SYSTEM))

    # Append OS name to the binary
    asset_old_path = glob('dist/*')[0]
    name, ext = os.path.splitext(os.path.basename(asset_old_path))
    asset_path = 'dist/{}-{}{}'.format(name, SYSTEM, ext)

    session.run('mv', '-f', asset_old_path, asset_path, external=True, **run_kwargs)

    # Set outputs for GitHub Actions
    if CI:
        # Path have to be specified with unix style slashes even for windows,
        # otherwise glob won't find files on windows in action-gh-release.
        print('asset_path=dist/*')


def _calculate_hashes(
    file_path: pathlib.Path,
    algorithms: List[str],
) -> List['hashlib._Hash']:  # noqa
    read_size = 1024 * 1024
    hash_structures = [hashlib.new(algo) for algo in algorithms]

    with open(file_path, 'rb') as f:
        while True:
            buffer = f.read(read_size)
            if not buffer:
                break

            for hash_struct in hash_structures:
                hash_struct.update(buffer)

    return hash_structures


def _save_hashes(output_file: pathlib.Path, hashes: List['hashlib._Hash']) -> None:  # noqa
    longest_algo_name = max([len(elem.name) for elem in hashes])
    line_format = '{algo:<%s} {hash_value}' % longest_algo_name

    output_lines = []
    for hash_struct in hashes:
        hash_value = hash_struct.hexdigest()
        output_lines.append(line_format.format(algo=hash_struct.name, hash_value=hash_value))

    output_file.write_bytes('\n'.join(output_lines).encode('ascii'))


@nox.session(python=PYTHON_DEFAULT_VERSION)
def make_dist_digest(_session):
    wanted_algos = ['sha256', 'sha512', 'sha3_256', 'sha3_512']
    available_algos = [algo for algo in wanted_algos if algo in hashlib.algorithms_available]

    directory = pathlib.Path('dist')
    glob_match = '*'

    hashes_file_suffix = '_hashes'
    did_find_any_file = False

    for dist_file in directory.glob(glob_match):
        if dist_file.stem.endswith(hashes_file_suffix):
            continue

        hashes_list = _calculate_hashes(dist_file, available_algos)

        output_file = dist_file.with_stem(dist_file.name + hashes_file_suffix).with_suffix('.txt')
        _save_hashes(output_file, hashes_list)

        did_find_any_file = True

    if not did_find_any_file:
        raise RuntimeError(
            f'No file found in {str(directory / glob_match)}, but was expected to find some.'
        )


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
