#!/usr/bin/env python3

from setuptools import setup, find_packages
from pathlib import Path

def read(path):
    with open(path) as input_file:
        return input_file.read()


name = "vzclient"


version_file = Path(__file__).parent / Path(*name.split('.')) / "version.py"
version_file_vars = {}
exec(read(version_file), version_file_vars)
release = version_file_vars['version']
version = ".".join(release.split('.')[:2])

try:
    from sphinx.setup_command import BuildDoc
    build_doc = {
        'cmdclass': {'build_sphinx': BuildDoc},
        'command_options': {
            'build_sphinx': {
                'project': ('setup.py', name),
                'version': ('setup.py', version),
                'release': ('setup.py', release),
                'source_dir': ('setup.py', 'doc/source')
            }
        }
    }
except ImportError:
    build_doc={}


setup(
    name=name,
    version=release,
    packages=find_packages(exclude=['tests']),
    scripts=[],
    install_requires=[ # See https://packaging.python.org/discussions/install-requires-vs-requirements/
        'setuptools',
        'sphinx',
        'sphinx-rtd-theme',
        'autobahn',
        'requests',
        'aiomysql',
        'aioinflux'
    ],
    author='claashk',
    author_email='claashk@xxx',
    description='A client for the volkszaehler environment',
    long_description = read('README.md'),
    long_description_content_type="text/markdown",
    keywords='Volkszaehler Asyncio',
    url='https://github.com/claashk/vzclient',
    project_urls={'Repository': 'https://github.com/claashk/vzclient'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta"
    ],
    test_suite='tests',
    python_requires=">=3.6",
    #zip_safe=False,
    **build_doc
)
 
