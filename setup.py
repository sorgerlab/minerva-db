""" Minerva Python Library
"""
import os
from configparser import ConfigParser
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, 'README.md')) as f:
    README = f.read()

REQUIRES = [
    'marshmallow==3.0.0b11',
    'marshmallow-sqlalchemy',
    'mccabe',
    # http://initd.org/psycopg/docs/install.html#binary-install-from-pypi
    'psycopg2-binary',
    'py',
    'PyYAML',
    'SQLAlchemy',
    'stringcase'
]

TEST_REQUIRES = [
    'pytest',
    'docker',
    'requests',
    'factory_boy'
]


def read_version():
    """
    Returns:
        Version string of this module
    """
    config = ConfigParser()
    config.read('setup.cfg')
    return config.get('metadata', 'version')


VERSION = read_version()
DESCRIPTION = 'minerva DB'
AUTHOR = 'D.P.W. Russell'
EMAIL = 'douglas_russell@hms.harvard.edu'
LICENSE = 'MIT'
HOMEPAGE = 'https://github.com/sorgerlab/minerva-db'

setup(
    name='minerva-db',
    version=VERSION,
    package_dir={'': 'src'},
    description=DESCRIPTION,
    long_description=README,
    packages=find_packages('src'),
    include_package_data=True,
    install_requires=REQUIRES,
    setup_requires=['pytest-runner'],
    tests_require=TEST_REQUIRES,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: Visualization'
    ],
    author=AUTHOR,
    author_email=EMAIL,
    license=LICENSE,
    url=HOMEPAGE,
    download_url='%s/archive/v%s.tar.gz' % (HOMEPAGE, VERSION),
    keywords=['minerva', 'database', 'library', 'microscopy'],
    zip_safe=False
)
