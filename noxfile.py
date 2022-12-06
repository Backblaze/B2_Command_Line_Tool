######################################################################
#
# File: noxfile.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import datetime
import os
import pathlib

import pkg_resources
import platform
import subprocess

from glob import glob
from typing import Tuple

import nox

import b2.version

CI = os.environ.get('CI') is not None
CD = CI and (os.environ.get('CD') is not None)
INSTALL_SDK_FROM = os.environ.get('INSTALL_SDK_FROM')
NO_STATICX = os.environ.get('NO_STATICX') is not None
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')
NO_INSTALL = os.environ.get('NO_INSTALL') is not None

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
    'pyinstaller==4.7.0',
    "patchelf-wrapper==1.2.0;platform_system=='Linux'",
    "staticx==0.13.5;platform_system=='Linux'",
]

OSX_BUNDLE_IDENTIFIER = 'com.backblaze.b2'
OSX_BUNDLE_ENTITLEMENTS = 'contrib/macos/entitlements.plist'

WINDOWS_TIMESTAMP_SERVER = 'http://timestamp.digicert.com'
WINDOWS_SIGNTOOL_PATH = 'C:/Program Files (x86)/Windows Kits/10/bin/10.0.17763.0/x86/signtool.exe'

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

    arg = '.'
    if extras:
        arg += '[%s]' % ','.join(extras)

    if not NO_INSTALL:
        session.run('pip', 'install', '-e', arg)

    if INSTALL_SDK_FROM:
        cwd = os.getcwd()
        os.chdir(INSTALL_SDK_FROM)
        session.run('pip', 'uninstall', 'b2sdk', '-y')
        session.run('python', 'setup.py', 'develop')
        os.chdir(cwd)
    elif CI and not CD:
        # In CI, install B2 SDK from the master branch
        session.run(
            'pip', 'install', 'git+https://github.com/Backblaze/b2-sdk-python.git#egg=b2sdk'
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
    install_myself(session)
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
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    #session.run('pytest', '-s', '-x', '-v', '-n', '4', *session.posargs, 'test/integration')
    session.run(
        'pytest', '-s', '-x', '-v', '-W', 'ignore::DeprecationWarning:rst2ansi.visitor:',
        *session.posargs, 'test/integration'
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
    session.run('pip', 'install', *REQUIREMENTS_BUILD)
    session.run('python', 'setup.py', 'check', '--metadata', '--strict')
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True)
    session.run('python', 'setup.py', 'sdist', *session.posargs)

    # Set outputs for GitHub Actions
    if CI:
        asset_path = glob('dist/*')[0]
        print('::set-output name=asset_path::', asset_path, sep='')

        version = os.environ['GITHUB_REF'].replace('refs/tags/v', '')
        print('::set-output name=version::', version, sep='')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def bundle(session):
    """Bundle the distribution."""
    session.run('pip', 'install', *REQUIREMENTS_BUNDLE)
    session.run('rm', '-rf', 'build', 'dist', 'b2.egg-info', external=True)
    install_myself(session)

    if SYSTEM == 'darwin':
        session.posargs.extend(['--osx-bundle-identifier', OSX_BUNDLE_IDENTIFIER])

    session.run('pyinstaller', '--onefile', *session.posargs, 'b2.spec')

    if SYSTEM == 'linux' and not NO_STATICX:
        session.run(
            'staticx', '--no-compress', '--strip', '--loglevel', 'INFO', 'dist/b2', 'dist/b2-static'
        )
        session.run('mv', '-f', 'dist/b2-static', 'dist/b2', external=True)

    # Set outputs for GitHub Actions
    if CI:
        asset_path = glob('dist/*')[0]
        print('::set-output name=asset_path::', asset_path, sep='')


@nox.session(python=False)
def sign(session):
    """Sign the bundled distribution (macOS and Windows only)."""

    def sign_darwin(cert_name):
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
            '--sign',
            cert_name,
            'dist/b2',
            external=True
        )
        session.run('codesign', '--verify', '--verbose', 'dist/b2', external=True)

    def sign_windows(cert_file, cert_password):
        session.run('certutil', '-f', '-p', cert_password, '-importpfx', cert_file)
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
            external=True
        )
        session.run(WINDOWS_SIGNTOOL_PATH, 'verify', '/pa', '/all', 'dist/b2.exe', external=True)

    if SYSTEM == 'darwin':
        try:
            certificate_name, = session.posargs
        except ValueError:
            session.error('pass the certificate name as a positional argument')
            return

        sign_darwin(certificate_name)
    elif SYSTEM == 'windows':
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

    session.run('mv', '-f', asset_old_path, asset_path, external=True)

    # Set outputs for GitHub Actions
    if CI:
        asset_path = glob('dist/*')[0]
        print('::set-output name=asset_path::', asset_path, sep='')


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


def _read_readme_name_and_description() -> Tuple[str, str]:
    # Get name and the description from the readme. First line is assumed to be the project name,
    # second contains list of all different checks. Third one and the following contains some description.
    # We assume that description can be multiline, and it ends with an empty line.
    with open('README.md', 'r') as f:
        non_empty_lines = 0
        full_name = None
        description_parts = []

        for line_with_ends in f.readlines():
            line = line_with_ends.strip()
            if len(line) == 0:
                # If we found an empty line after we got anything for our description – finish.
                if len(description_parts) > 0:
                    break
                continue

            non_empty_lines += 1

            if non_empty_lines == 1:
                # Markdown header starts with some "# ", we strip everything up to first space.
                full_name = line.split(' ', maxsplit=1)[1]

            if non_empty_lines < 3:
                continue

            description_parts.append(line)

    return full_name, ' '.join(description_parts)


def _get_git_ref() -> str:
    result = subprocess.run(['git', 'rev-parse', 'HEAD'], stdout=subprocess.PIPE)
    assert result.returncode == 0
    return result.stdout.decode('ascii').strip()


@nox.session(python=PYTHON_DEFAULT_VERSION)
def docker(session):
    """Build the docker image."""
    build(session)

    full_name, description = _read_readme_name_and_description()
    vcs_ref = _get_git_ref()
    built_distribution = list(pathlib.Path('.').glob('dist/*'))[0]

    username = 'b2'
    homedir = f'/{username}'

    docker_file_template = [
        # First layer, actual library.
        f'FROM python:{session.python}-slim as base',
        # Even if we point to a different home directory, we'll get skeleton copied.
        f'RUN ["adduser", "--no-create-home", "--disabled-password", "--force-badname", "--quiet", "{username}"]',
        f'RUN ["usermod", "--home", "{homedir}", "{username}"]',
        f'USER {username}',
        '',
        # Labels. These are based on http://label-schema.org/
        'LABEL vendor=Backblaze',
        f'LABEL name="{full_name}"',
        f'LABEL description="{description}"',
        f'LABEL version="{b2.version.VERSION}"',
        'LABEL url="https://www.backblaze.com"',
        # TODO: consider fetching it from `git ls-remote --get-url origin`
        'LABEL vcs-url="https://github.com/Backblaze/B2_Command_Line_Tool"',
        f'LABEL vcs-ref="{vcs_ref}"',
        f'LABEL build-date-iso8601="{datetime.datetime.utcnow().isoformat()}"',
        '',
        # Installation.
        f'WORKDIR {homedir}',
        # Not using ADD because I don't want this unpacked.
        f'COPY {built_distribution.as_posix()} .',
        # We can install all the extras here as well and then use multi-stage images to provide dependencies.
        f'RUN ["pip", "install", "{built_distribution.relative_to("dist").as_posix()}"]',
        # Ensure that we can run installed packages.
        f'ENV PATH={homedir}/.local/bin:$PATH',
        '',

        # Second layer, tests. All tests are copied, but we're running only units for now.
        'FROM base as test',
        '',
        # Environment variables. Both are unset and here just to notify user that these are needed.
        'ENV B2_TEST_APPLICATION_KEY=""',
        'ENV B2_TEST_APPLICATION_KEY_ID=""',
        '',
        'WORKDIR /test',
        'COPY test ./test',
        'COPY noxfile.py .',
        # Required for some tests.
        'COPY README.md .',
        'RUN ["pip", "install", "nox"]',
        # Ensuring that `pip install -e .` will be invoked. We're on
        # a special image that already has the latest version installed.
        'ENV NO_INSTALL=1',
        'CMD ["nox", "--no-venv", "-s", "test"]',
        '',

        # Final layer, production image.
        f'FROM base',
        '',
        # Environment variables. Both are unset and here just to notify user that these are needed.
        'ENV B2_APPLICATION_KEY=""',
        'ENV B2_APPLICATION_KEY_ID=""',
        '',
        f'ENTRYPOINT ["b2"]',
        f'CMD ["--help"]',
    ]

    with open('Dockerfile', 'w') as f:
        f.write('\n'.join(docker_file_template))


@nox.session(python=PYTHON_DEFAULT_VERSION)
def docker_test(session):
    """Run unittests against the docker image."""
