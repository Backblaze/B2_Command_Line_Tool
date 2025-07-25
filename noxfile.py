######################################################################
#
# File: noxfile.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime
import hashlib
import os
import pathlib
import platform
import re
import string
import subprocess

import nox

# Required for PDM to use nox's virtualenvs
os.environ['PDM_IGNORE_SAVED_PYTHON'] = '1'
os.environ['PDM_NO_LOCK'] = '1'

UPSTREAM_REPO_URL = 'git@github.com:Backblaze/B2_Command_Line_Tool.git'

CI = os.environ.get('CI') is not None
CD = CI and (os.environ.get('CD') is not None)
INSTALL_SDK_FROM = os.environ.get('INSTALL_SDK_FROM')
NO_STATICX = os.environ.get('NO_STATICX') is not None
NOX_PYTHONS = os.environ.get('NOX_PYTHONS')

PYTHON_VERSIONS = (
    [
        'pypy3.9',
        'pypy3.10',
        '3.8',
        '3.9',
        '3.10',
        '3.11',
        '3.12',
        '3.13',
    ]
    if NOX_PYTHONS is None
    else NOX_PYTHONS.split(',')
)


def _detect_python_nox_id() -> str:
    major, minor, *_ = platform.python_version_tuple()
    python_nox_id = f'{major}.{minor}'
    if platform.python_implementation() == 'PyPy':
        python_nox_id = f'pypy{python_nox_id}'
    return python_nox_id


if CI and not NOX_PYTHONS:
    # this is done to allow it to work even if `nox -p` was passed to nox
    PYTHON_VERSIONS = [_detect_python_nox_id()]
    print(f'CI job mode; using provided interpreter only; PYTHON_VERSIONS={PYTHON_VERSIONS!r}')

PYTHON_DEFAULT_VERSION = PYTHON_VERSIONS[-2] if len(PYTHON_VERSIONS) > 1 else PYTHON_VERSIONS[0]

PY_PATHS = ['b2', 'test', 'noxfile.py']

DOCKER_TEMPLATE = pathlib.Path('docker/Dockerfile.template')

SYSTEM = platform.system().lower()

WINDOWS_TIMESTAMP_SERVER = 'http://timestamp.digicert.com'
WINDOWS_SIGNTOOL_PATH = 'C:/Program Files (x86)/Windows Kits/10/bin/10.0.17763.0/x86/signtool.exe'

nox.options.reuse_existing_virtualenvs = not CI
nox.options.sessions = [
    'lint',
    'test',
]

PYTEST_GLOBAL_ARGS = []
if CI:
    PYTEST_GLOBAL_ARGS.append('-vv')


def pdm_install(
    session: nox.Session, *groups: str, dev: bool = True, editable: bool = False
) -> None:
    args = []
    if not dev:
        args.append('--prod')
    if not editable:
        args.append('--no-editable')
    for group in groups:
        args.extend(['--group', group])
    session.run('pdm', 'install', *args, external=True)
    if INSTALL_SDK_FROM:
        session.run('pip', 'install', INSTALL_SDK_FROM, external=True)


def github_output(name, value, *, secret=False):
    gh_output_path = os.environ.get('GITHUB_OUTPUT')
    if secret:
        print(f'::add-mask::{value}')
    if gh_output_path:
        with open(gh_output_path, 'a') as file:
            file.write(f'{name}={value}\n')
    else:
        print(f"github_output {name}={'******' if secret else value}")


def get_version_key(path: pathlib.Path) -> int:
    version_name = path.name
    # There is no version 0, thus we can provide it to the element starting with an underscore.
    if version_name.startswith('_'):
        return 0

    version_match = re.match(r'[_]*b2v(\d+)', version_name)
    assert version_match, f'Version {version_name} does not match pattern B2Cli version pattern.'
    version_number = int(version_match.group(1))
    return version_number


def get_versions() -> list[str]:
    """
    "Almost" a copy of b2/_internal/version_listing.py:get_versions(), because importing
    the file directly seems impossible from the noxfile.
    """
    # This sorting ensures that:
    # - the first element is the latest unstable version (starts with an underscore)
    # - the last element is the latest stable version (highest version number)
    return [
        path.name
        for path in sorted(
            (pathlib.Path(__file__).parent / 'b2' / '_internal').glob('*b2v*'),
            key=get_version_key,
        )
        if (path / '__init__.py').exists()
    ]


@nox.session(name='format', python=PYTHON_DEFAULT_VERSION)
def format_(session):
    """Lint the code and apply fixes in-place whenever possible."""
    pdm_install(session, 'format')
    session.run('ruff', 'check', '--fix', *PY_PATHS)
    session.run('ruff', 'format', *PY_PATHS)
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
    pdm_install(session, 'lint', 'doc', 'full', 'license')
    session.run('ruff', 'check', *PY_PATHS)
    session.run('ruff', 'format', *PY_PATHS)
    # session.run(
    #     'docformatter',
    #     '--check',
    #     '--recursive',
    #     '--wrap-summaries=100',
    #     '--wrap-descriptions=100',
    #     *PY_PATHS,
    # )

    session.run('pytest', 'test/static', *PYTEST_GLOBAL_ARGS)
    session.run('liccheck', '-s', 'pyproject.toml')
    session.run('pdm', 'lock', '--check', external=True)


@nox.session(python=PYTHON_VERSIONS)
def unit(session):
    """Run unit tests."""
    pdm_install(session, 'test')

    command = [
        'pytest',
        '-n',
        'auto',
        '--cov=b2',
        '--cov-branch',
        '--cov-report=xml',
        '--doctest-modules',
        *PYTEST_GLOBAL_ARGS,
        *session.posargs,
        'test/unit',
    ]

    versions = get_versions()
    session.run(*command, '--cli', versions[0])
    command.append('--cov-append')
    if not session.posargs:
        session.notify('cover')

    for cli_version in versions[1:]:
        session.run(*command, '--cli', cli_version)


def run_integration_test(session, pytest_posargs):
    """Run integration tests."""
    pdm_install(session, 'license', 'test')

    command = [
        'pytest',
        'test/integration',
        '-n',
        '2' if CI else 'auto',
        '--log-level',
        'INFO',
        '-W',
        'ignore::DeprecationWarning:rst2ansi.visitor:',
        *PYTEST_GLOBAL_ARGS,
        *pytest_posargs,
    ]

    # sut can be provided explicitly (like in docker) or like `"--sut=path/b2"`.
    provided_sut = any('--sut' in elem for elem in pytest_posargs)

    # If `sut` was provided, we just run this one.
    # If not, we're running the test on all known versions.
    if provided_sut:
        session.run(*command)
    else:
        versions = get_versions()
        for cli_version in versions:
            # If we're in a virtualenv, we want to extract the path to the executable
            # that's installed in the virtualenv.  This may not be elegant but shutil
            # gives us a cross-platform solution out of the box.
            exe_path = session.run(
                'python', '-c', f'import shutil; print(shutil.which("{cli_version}"))', silent=True
            ).strip()
            session.run(*command, '--sut', exe_path)


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
    pdm_install(session, 'test')
    session.run(
        'pytest',
        '-s',
        '-x',
        *PYTEST_GLOBAL_ARGS,
        *session.posargs,
        'test/integration/cleanup_buckets.py',
    )


@nox.session
def cover(session):
    """Perform coverage analysis."""
    pdm_install(session, 'test')
    session.run('coverage', 'report', '--fail-under=75', '--show-missing', '--skip-covered')
    session.run('coverage', 'erase')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def build(session):
    """Build the distribution."""
    session.run('nox', '-s', 'dump_license', '-fb', 'venv', external=True)
    session.run('pdm', 'build', external=True)

    # Path have to be specified with unix style slashes even for windows,
    # otherwise glob won't find files on windows in action-gh-release.
    github_output('asset_path', 'dist/*')

    if CI:
        version = os.environ['GITHUB_REF'].replace('refs/tags/v', '')
    else:
        version = subprocess.check_output(['git', 'describe', '--tags']).decode().strip()

    github_output('version', version)


@nox.session(python=PYTHON_DEFAULT_VERSION)
def dump_license(session: nox.Session):
    pdm_install(session, 'license', editable=True)
    session.run('b2', 'license', '--dump', '--with-packages')


@nox.session(python=PYTHON_DEFAULT_VERSION)
def bundle(session: nox.Session):
    """Bundle the distribution."""

    # We're running dump_license in another session because:
    # 1. `b2 license --dump` dumps the licence where the module is installed.
    # 2. We don't want to install b2 as editable module in the current session
    #    because that would make `b2 versions` show the versions as editable.
    session.run('nox', '-s', 'dump_license', '-fb', 'venv', external=True)
    pdm_install(session, 'bundle', 'full')

    template_spec = string.Template(pathlib.Path('b2.spec.template').read_text())
    versions = get_versions()

    # It is assumed that the last element will be the "latest stable".
    for binary_name, version in [('b2', versions[-1])] + list(zip(versions, versions)):
        spec = template_spec.safe_substitute(
            {
                'VERSION': version,
                'NAME': binary_name,
            }
        )
        pathlib.Path(f'{binary_name}.spec').write_text(spec)

        session.run('pyinstaller', *session.posargs, f'{binary_name}.spec')

        if SYSTEM == 'linux' and not NO_STATICX:
            session.run(
                'staticx',
                '--no-compress',
                '--strip',
                '--loglevel',
                'INFO',
                f'dist/{binary_name}',
                f'dist/{binary_name}-static',
            )
            session.run(
                'mv',
                '-f',
                f'dist/{binary_name}-static',
                f'dist/{binary_name}',
                external=True,
            )

    # Path have to be specified with unix style slashes even for windows,
    # otherwise glob won't find files on windows in action-gh-release.
    github_output('asset_path', 'dist/*')

    # Note: this should pick the shortest named executable from the directory.
    # But, for yet unknown reason, the `./dist/b2` doesn't play well with `--sut` and the autocomplete.
    # For this reason, we're returning here the "latest, stable version" instead.
    # This current implementation works fine up until version 10, when it will break.
    # By that time, we should have come back to picking the shortest named binary (`b2`) up.
    executable = max(
        str(path) for path in pathlib.Path('dist').glob('*') if not path.name.startswith('_')
    )
    github_output('sut_path', executable)


@nox.session(python=False)
def sign(session):
    """Sign the bundled distribution (macOS and Windows only)."""

    def sign_windows(keypair_alias, cert_fingerprint):
        for binary_name in ['b2'] + get_versions():
            binary_path = f'dist/{binary_name}.exe'

            # Sign the binary
            session.run(
                'smctl',
                'sign',
                '--keypair-alias',
                keypair_alias,
                '--input',
                binary_path,
                external=True,
            )

            # Verify the signature
            session.run(
                'smctl',
                'sign',
                'verify',
                '--fingerprint',
                cert_fingerprint,
                '--input',
                binary_path,
                external=True,
            )

    if SYSTEM == 'windows':
        try:
            sm_keypair_alias, sm_cert_fingerprint = session.posargs
        except ValueError:
            session.error('pass the keypair alias and the cert fingerprint as positional arguments')
            return

        sign_windows(sm_keypair_alias, sm_cert_fingerprint)
    elif SYSTEM == 'linux':
        session.log('signing is not supported for Linux')
    else:
        session.error(f'unrecognized platform: {SYSTEM}')

    # Append OS name to all the binaries.
    for asset in pathlib.Path('dist').glob('*'):
        name = asset.stem
        ext = asset.suffix
        asset_path = f'dist/{name}-{SYSTEM}{ext}'
        session.run('mv', '-f', asset, asset_path, external=True)

    # Path have to be specified with unix style slashes even for windows,
    # otherwise glob won't find files on windows in action-gh-release.
    github_output('asset_path', 'dist/*')


def _calculate_hashes(
    file_path: pathlib.Path,
    algorithms: list[str],
) -> list[hashlib._Hash]:  # noqa
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


def _save_hashes(output_file: pathlib.Path, hashes: list[hashlib._Hash]) -> None:  # noqa
    longest_algo_name = max(len(elem.name) for elem in hashes)
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
    pdm_install(session, 'doc')
    session.cd('doc')
    sphinx_args = ['-b', 'html', '-T', '-W', 'source', 'build/html']
    session.run('rm', '-rf', 'build', external=True)

    if not session.interactive:
        session.run('sphinx-build', *sphinx_args)
        # session.notify('doc_cover')  #  disabled due to https://github.com/sphinx-doc/sphinx/issues/11678
    else:
        sphinx_args[-2:-2] = [
            '-E',
            '--open-browser',
            '--watch',
            '../b2',
            '--ignore',
            '*.pyc',
            '--ignore',
            '*~',
            '--ignore',
            'source/subcommands/*',
        ]
        session.run('sphinx-autobuild', *sphinx_args)


@nox.session
def doc_cover(session):
    """
    Perform coverage analysis for the documentation.

    At the time of writing B2 CLI does not have object documentation, hence this always returns 0 out 0 objects.
    Which errors out in Sphinx 7.2 (https://github.com/sphinx-doc/sphinx/issues/11678).
    """
    pdm_install(session, 'doc')
    session.cd('doc')
    sphinx_args = ['-b', 'coverage', '-T', '-W', 'source', 'build/coverage']
    session.run('sphinx-build', *sphinx_args)


def _read_readme_name_and_description() -> tuple[str, str]:
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
    pdm_install(session)

    # This string is like `b2 command line tool, version <sem-ver-string>`
    version = session.run('b2', 'version', '--short', silent=True).strip()

    dist_path = 'dist'

    full_name, description = _read_readme_name_and_description()
    vcs_ref = session.run('git', 'rev-parse', 'HEAD', external=True, silent=True).strip()
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
    user_id = session.run('id', '-u', silent=True, external=True).strip()
    group_id = session.run('id', '-g', silent=True, external=True).strip()
    docker_run_cmd = f'docker run -i --user {user_id}:{group_id} -v /tmp:/tmp:rw --env-file ENVFILE'
    run_integration_test(
        session,
        [
            '--sut',
            f'{docker_run_cmd} {image_tag}',
            '--env-file-cmd-placeholder',
            'ENVFILE',
        ],
    )
    for binary_name in get_versions():
        run_integration_test(
            session,
            [
                '--sut',
                f'{docker_run_cmd} {image_tag} {binary_name}',
                '--env-file-cmd-placeholder',
                'ENVFILE',
            ],
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

    requirements = session.run('pdm', 'export', '--no-hashes', silent=True)
    # if b2sdk requirement points to git, it won't have a version definition b2sdk==
    assert ('b2sdk==' in requirements) and (
        'git+' not in requirements
    ), 'release version must depend on released b2sdk version'

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

    pdm_install(session, 'release')
    session.run('towncrier', 'build', '--yes', '--version', version)

    session.log(
        f'CHANGELOG updated, changes ready to commit and push\n'
        f'    git remote add upstream {UPSTREAM_REPO_URL!r} 2>/dev/null || git remote get-url upstream\n'
        f'    git commit -m "release {version}"\n'
        f'    git push upstream {current_branch}\n'
        f'Wait for a CI workflow to complete successfully, before triggering CD by pushing a tag.\n'
        f'    git tag v{version}\n'
        f'    git push upstream v{version}\n'
        f'Wait for a CD workflow to complete successfully, indicates the release is done.'
    )


def load_allowed_change_types(
    project_toml: pathlib.Path = pathlib.Path('./pyproject.toml'),
) -> set[str]:
    """
    Load the list of allowed change types from the pyproject.toml file.
    """
    import tomllib

    configuration = tomllib.loads(project_toml.read_text())
    return set(entry['directory'] for entry in configuration['tool']['towncrier']['type'])


def is_changelog_filename_valid(filename: str, allowed_change_types: set[str]) -> tuple[bool, str]:
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
        return False, 'Doesn\'t follow the "<description>.<change_type>.md" pattern.'

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


def is_changelog_entry_valid(file_content: str) -> tuple[bool, str]:
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
