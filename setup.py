######################################################################
#
# File: setup.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

import platform
import sys

# To use a consistent encoding
from codecs import open
from importlib import import_module

# Always prefer setuptools over distutils
from setuptools import __version__ as setuptoolsversion
from setuptools import setup, find_packages

#require at least setuptools 20.2 for PEP 508 conditional dependency support
MIN_SETUPTOOLS_VERSION = (20, 2)
if tuple(int(x) for x in setuptoolsversion.split('.')[:2]) < MIN_SETUPTOOLS_VERSION:
    sys.exit(
        'setuptools %s.%s or later is required. To fix, try running: pip install "setuptools>=%s.%s"'
        % (MIN_SETUPTOOLS_VERSION * 2)
    )

# Get the long description from the README file
with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt', encoding='utf-8') as f:
    requirements = f.read().splitlines()

# Jython cannot handle extremely large blocks of code.
# requests 2.12.x that we rely on, relied on idna, which until 2.2.0 contained such block.
# https://github.com/kennethreitz/requests/issues/3711#issuecomment-268522266
if platform.system().lower().startswith('java'):
    requirements.append('idna>=2.2.0')

setup(
    name='b2',
    description='Command Line Tool for Backblaze B2',
    long_description=long_description,
    long_description_content_type='text/markdown',

    # The project's main homepage.
    url='https://github.com/Backblaze/B2_Command_Line_Tool',

    # Author details
    author='Backblaze, Inc.',
    author_email='support@backblaze.com',

    # Choose your license
    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        # ??? What are the right classifiers for a command-line tool? ???
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],

    # What does your project relate to?
    keywords='backblaze b2 cloud storage',

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=['contrib', 'doc', 'test*']),

    # Alternatively, if you want to distribute just a my_module.py, uncomment
    # this:
    #  py_modules=["my_module"],

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    dependency_links=[],
    install_requires=requirements,

    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[dev,test]
    extras_require={
        'doc':
            [
                'sphinx', 'sphinx-autobuild', 'sphinx_rtd_theme', 'sphinx-argparse',
                'sphinxcontrib-plantuml', 'sadisplay'
            ],
    },
    setup_requires=['setuptools_scm<6.0'],  # setuptools_scm>=6.0 doesn't support Python 3.5
    use_scm_version=True,

    # If there are data files included in your packages that need to be
    # installed, specify them here.
    package_data={'b2': ['requirements.txt', 'LICENSE']},

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages. See:
    # http://docs.python.org/3.9/distutils/setupscript.html#installing-additional-files # noqa
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    data_files=[
        #('my_data', ['data/data_file'])
    ],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': ['b2=b2.console_tool:main'],
    },
)
