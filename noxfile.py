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
import hashlib
import os
import pathlib
import platform
import re
import string
import subprocess
from glob import glob
from typing import List, Set, Tuple

import nox

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
    '3.12',
] if NOX_PYTHONS is None else NOX_PYTHONS.split(',')
PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-1]

PY_PATHS = ['b2', 'test', 'noxfile.py', 'setup.py']

DOCKER_TEMPLATE = pathlib.Path('./Dockerfile.template')
FILES_USED_IN_TESTS = ['README.md', 'CHANGELOG.md']

SYSTEM = platform.system().lower()

REQUIREMENTS_FORMAT = ['yapf==0.27', 'ruff==0.0.272']
REQUIREMENTS_LINT = REQUIREMENTS_FORMAT + ['pytest==6.2.5', 'liccheck==0.6.2']
REQUIREMENTS_RELEASE = ['towncrier==23.11.0']
REQUIREMENTS_TEST = [
    "pexpect==4.8.0",
    "pytest==6.2.5",
    "pytest-cov==3.0.0",
    'pytest-forked==1.4.0',
    'pytest-xdist==2.5.0',
    'backoff==2.1.2',
    'more_itertools==8.13.0',
]

# not pinned to test the latest version works
REQUIREMENTS_BUILD = [
    'setuptools',
]

REQUIREMENTS_BUNDLE = [
    'pyinstaller~=5.13',
    'pyinstaller-hooks-contrib>=2023.6',
    "patchelf-wrapper==1.2.0;platform_system=='Linux'",
    "staticx~=0.13.9;platform_system=='Linux'",
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


@nox.session(venv_backend='none')
def install(session):
    install_myself(session)


def install_myself(session, extras=None):
    """Install from the source."""

    arg = '.'
    if extras:
        arg += '[%s]' % ','.join(extras)

    # `--no-install` works only on `run_always` in case where there is a virtualenv available.
    # This is to be used also on the docker image during tests, where we have no venv, thus
    # we're ensuring that `--no-install` doesn't work on this installation.
    # Internal member is used, as there is no public interface for fetching options.
    if not session._runner.global_config.no_install:  # noqa
        session.run('pip', 'install', '-e', arg, **run_kwargs)

    if INSTALL_SDK_FROM:
        cwd = os.getcwd()
        os.chdir(INSTALL_SDK_FROM)
        session.run('pip', 'uninstall', 'b2sdk', '-y')
        session.run('pip', 'install', '-e', '.')
        os.chdir(cwd)
    elif CI and not CD:
        # In CI, install B2 SDK from the master branch
        session.run(
            'pip', 'install', 'git+https://github.com/Backblaze/b2-sdk-python.git#egg=b2sdk',
            **run_kwargs
        )


@nox.session(name='format', python=PYTHON_DEFAULT_VERSION)
def format_(session):
    """Lint the code and apply fixes in-place whenever possible."""
    session.run('pip', 'install', *REQUIREMENTS_FORMAT)
    # TODO: incremental mode for yapf
    session.run('yapf', '--in-place', '--parallel', '--recursive', *PY_PATHS)
    session.run('ruff', 'check', '--fix', *PY_PATHS)
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
    """Run linters in readonly mode."""
    install_myself(session)
    session.run('pip', 'install', *REQUIREMENTS_LINT)
    session.run('yapf', '--diff', '--parallel', '--recursive', *PY_PATHS)
    session.run('ruff', 'check', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--check',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )

    session.run('pytest', 'test/static')

    # Before checking licenses, create an updated requirements.txt file, which accepts any b2sdk version.  This way
    # the tool will still work if the SDK was installed from the master branch or a different directory.
    updated_requirements = os.path.join(session.create_tmp(), 'requirements.txt')
    with open('requirements.txt') as orig_req_file, \
            open(updated_requirements, 'w') as updated_req_file:
        for requirement in orig_req_file.readlines():
            if requirement.startswith("b2sdk"):
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


def run_integration_test(session, pytest_posargs):
    """Run integration tests."""
    install_myself(session, ['license'])
    session.run('pip', 'install', *REQUIREMENTS_TEST)
    session.run(
        'pytest',
        'test/integration',
        '-s',
        '-n',
        '2' if CI else 'auto',
        '--log-level',
        'INFO',
        '-W',
        'ignore::DeprecationWarning:rst2ansi.visitor:',
        *pytest_posargs,
    )


@nox.session(python=PYTHON_VERSIONS)
def integration(session):
    """Run integration tests."""
    run_integration_test(session, session.posargs)


@nox.session(python=PYTHON_VERSIONS)
def test(session):
    """Run all tests."""
    if session.python:
        session.notify(f'unit-{session.python}')
        session.notify(f'integration-{session.python}')
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
    session.run('pip', 'install', '-U', *REQUIREMENTS_BUILD, **run_kwargs)
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
    install_myself(session, ['license', 'full'])
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
        session.error(f'unrecognized platform: {SYSTEM}')

    # Append OS name to the binary
    asset_old_path = glob('dist/*')[0]
    name, ext = os.path.splitext(os.path.basename(asset_old_path))
    asset_path = f'dist/{name}-{SYSTEM}{ext}'

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
        # session.notify('doc_cover')  #  disabled due to https://github.com/sphinx-doc/sphinx/issues/11678
    else:
        sphinx_args[-2:-2] = [
            '-E', '--open-browser', '--watch', '../b2', '--ignore', '*.pyc', '--ignore', '*~'
        ]
        session.run('sphinx-autobuild', *sphinx_args)


@nox.session
def doc_cover(session):
    """
    Perform coverage analysis for the documentation.

    At the time of writing B2 CLI does not have object documentation, hence this always returns 0 out 0 objects.
    Which errors out in Sphinx 7.2 (https://github.com/sphinx-doc/sphinx/issues/11678).
    """
    install_myself(session, extras=['doc'])
    session.cd('doc')
    sphinx_args = ['-b', 'coverage', '-T', '-W', 'source', 'build/coverage']
    session.run('sphinx-build', *sphinx_args)


def _read_readme_name_and_description() -> Tuple[str, str]:
    """
    Get name and the description from the readme. First line is assumed to be the project name,
    second contains list of all different checks. Third one and the following contains some description.
    We assume that description can be multiline, and it ends with an empty line.

    An example of the content from README.md can look like this:

    ..note:
        # B2 Command Line Tool
        &nbsp;[![Continuous Integration](https://github.com/Backblaze/B2_Command_Line_Tool/ ... (a very long line)

        (a few empty lines)

        The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

        This program provides command-line access to the B2 service.

    From this we should parse the following:
    "B2 Command Line Tool" as the name and
    "The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage." as the description.
    """
    with open('README.md') as f:
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


@nox.session(python=PYTHON_DEFAULT_VERSION)
def generate_dockerfile(session):
    """Generate Dockerfile from Dockerfile.template"""
    build(session)

    install_myself(session)
    # This string is like `b2 command line tool, version <sem-ver-string>`
    version = session.run('b2', 'version', '--short', silent=True).strip()

    dist_path = 'dist'

    full_name, description = _read_readme_name_and_description()
    vcs_ref = session.run("git", "rev-parse", "HEAD", external=True, silent=True).strip()
    built_distribution = list(pathlib.Path('.').glob(f'{dist_path}/*'))[0]

    template_mapping = dict(
        python_version=PYTHON_DEFAULT_VERSION,
        vendor='Backblaze',
        name=full_name,
        description=description,
        version=version,
        url='https://www.backblaze.com',
        # TODO: consider fetching it from `git ls-remote --get-url origin`
        vcs_url='https://github.com/Backblaze/B2_Command_Line_Tool',
        vcs_ref=vcs_ref,
        build_date=datetime.datetime.utcnow().isoformat(),
        tar_path=dist_path,
        tar_name=built_distribution.name,
    )

    template_file = DOCKER_TEMPLATE.read_text()
    template = string.Template(template_file)
    dockerfile = template.substitute(template_mapping)
    pathlib.Path('./Dockerfile').write_text(dockerfile)


def run_docker_tests(session, image_tag):
    """Run unittests against a docker image."""
    run_integration_test(
        session, [
            "--sut",
            f"docker run -i -v b2:/root -v /tmp:/tmp:rw "
            f"--env-file ENVFILE {image_tag}",
            "--env-file-cmd-placeholder",
            "ENVFILE",
        ]
    )


@nox.session(python=PYTHON_DEFAULT_VERSION)
def docker_test(session):
    """Run unittests against a docker image."""
    if session.posargs:
        image_tag = session.posargs[0]
    else:
        raise ValueError('Provide -- {docker_image_tag}')
    run_docker_tests(session, image_tag)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build_and_test_docker(session):
    """
    For running locally, CI uses a different set of sessions
    """
    test_image_tag = 'b2:test'
    generate_dockerfile(session)
    session.run('docker', 'build', '-t', test_image_tag, '.', external=True)
    run_docker_tests(session, test_image_tag)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def make_release_commit(session):
    """
    Runs `towncrier build`, commits changes, tags, all that is left to do is pushing
    """
    if session.posargs:
        version = session.posargs[0]
    else:
        session.error('Provide -- {release_version} (X.Y.Z - without leading "v")')

    if not re.match(r'^\d+\.\d+\.\d+$', version):
        session.error(
            f'Provided version="{version}". Version must be of the form X.Y.Z where '
            f'X, Y and Z are integers'
        )

    local_changes = subprocess.check_output(['git', 'diff', '--stat'])
    if local_changes:
        session.error('Uncommitted changes detected')

    current_branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode()
    if current_branch != 'master':
        session.log('WARNING: releasing from a branch different than master')

    session.run('pip', 'install', *REQUIREMENTS_RELEASE)
    session.run('towncrier', 'build', '--yes', '--version', version)

    session.log(
        f'CHANGELOG updated, changes ready to commit and push\n'
        f'    git commit -m "release {version}"\n'
        f'    git tag v{version}\n'
        f'    git push {{UPSTREAM_NAME}} v{version}\n'
        f'    git push {{UPSTREAM_NAME}} {current_branch}'
    )


def load_allowed_change_types(project_toml: pathlib.Path = pathlib.Path('./pyproject.toml')
                             ) -> Set[str]:
    """
    Load the list of allowed change types from the pyproject.toml file.
    """
    import tomllib
    configuration = tomllib.loads(project_toml.read_text())
    return set(entry['directory'] for entry in configuration['tool']['towncrier']['type'])


def is_changelog_filename_valid(filename: str, allowed_change_types: Set[str]) -> Tuple[bool, str]:
    """
    Validates whether the given filename matches our rules.
    Provides information about why it doesn't match them.
    """
    error_reasons = []

    wanted_extension = 'md'
    try:
        description, change_type, extension = filename.rsplit('.', maxsplit=2)
    except ValueError:
        # Not enough values to unpack.
        return False, "Doesn't follow the \"<description>.<change_type>.md\" pattern."

    # Check whether the filename ends with .md.
    if extension != wanted_extension:
        error_reasons.append(f"Doesn't end with {wanted_extension} extension.")

    # Check whether the change type is valid.
    if change_type not in allowed_change_types:
        error_reasons.append(
            f"Change type '{change_type}' doesn't match allowed types: {allowed_change_types}."
        )

    # Check whether the description makes sense.
    try:
        int(description)
    except ValueError:
        if description[0] != '+':
            error_reasons.append("Doesn't start with a number nor a plus sign.")

    return len(error_reasons) == 0, ' / '.join(error_reasons) if error_reasons else ''


def is_changelog_entry_valid(file_content: str) -> Tuple[bool, str]:
    """
    We expect the changelog entry to be a valid sentence in the English language.
    This includes, but not limits to, providing a capital letter at the start
    and the full-stop character at the end.

    Note: to do this "properly", tools like `nltk` and `spacy` should be used.
    """
    error_reasons = []

    # Check whether the first character is a capital letter.
    # Not allowing special characters nor numbers at the very start.
    if not file_content[0].isalpha() or not file_content[0].isupper():
        error_reasons.append('The first character is not a capital letter.')

    # Check if the last character is a full-stop character.
    if file_content.strip()[-1] != '.':
        error_reasons.append('The last character is not a full-stop character.')

    return len(error_reasons) == 0, ' / '.join(error_reasons) if error_reasons else ''


@nox.session(python=PYTHON_DEFAULT_VERSION)
def towncrier_check(session):
    """
    Check whether all the entries in the changelog.d follow the expected naming convention
    as well as some basic rules as to their format.
    """
    expected_non_md_files = {'.gitkeep'}
    allowed_change_types = load_allowed_change_types()

    is_error = False

    for filename in pathlib.Path('./changelog.d/').glob('*'):
        # If that's an expected file, it's all right.
        if filename.name in expected_non_md_files:
            continue

        # Check whether the file matches the expected pattern.
        is_valid, error_message = is_changelog_filename_valid(filename.name, allowed_change_types)
        if not is_valid:
            session.log(f"File {filename.name} doesn't match the expected pattern: {error_message}")
            is_error = True
            continue

        # Check whether the file isn't too big.
        if filename.lstat().st_size > 16 * 1024:
            session.log(
                f'File {filename.name} content is too big – it should be smaller than 16kB.'
            )
            is_error = True
            continue

        # Check whether the file can be loaded as UTF-8 file.
        try:
            file_content = filename.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            session.log(f'File {filename.name} is not a valid UTF-8 file.')
            is_error = True
            continue

        # Check whether the content of the file is anyhow valid.
        is_valid, error_message = is_changelog_entry_valid(file_content)
        if not is_valid:
            session.log(f'File {filename.name} is not a valid changelog entry: {error_message}')
            is_error = True
            continue

    if is_error:
        session.error(
            'Found errors in the changelog.d directory. Check logs above for more information'
        )
