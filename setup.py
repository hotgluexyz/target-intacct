#!/usr/bin/env python
from setuptools import find_packages, setup

setup(
    name='target-intacct',
    version='0.0.7',
    description='hotglue target for posting data to the Intacct API.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='hotglue',
    url='https://github.com/hotgluexyz/target-intacct',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    python_requires='>=3.7.1',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'backoff',
        'requests>=2.20.0',

        'pandas==1.3.5; python_version < "3.9"',
        'pandas>=2.3.3; python_version >= "3.9"',

        'singer-python>=5.0.12',
        'xmltodict==0.12.0',
    ],
    entry_points='''
        [console_scripts]
        target-intacct=target_intacct:main
    ''',
)
