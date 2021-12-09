#!/usr/bin/env python

from distutils.core import setup

setup(name='easy_minio',
      version='0.3',
      description='A minio python API wrapper',
      author='Thyrix Yang',
      author_email='thyrixyang@gmail.com',
      url='https://github.com/ThyrixYang/easy_minio',
      packages=['easy_minio'],
      package_dir={'easy_minio': 'easy_minio'},
      install_requires=['minio', 'pytest', 'sqlitedict', 'xxhash'],
)