# coding:utf8
from setuptools import setup

import easyctp

setup(
    name='easyctp',
    version=easyctp.__version__,
    description='A utility for Chain ctp',
    author='shidenggui',
    author_email='longlyshidenggui@gmail.com',
    license='BSD',
    url='https://github.com/shidenggui/easytrader',
    keywords='China stock trade',
    install_requires=[
        'requests',
        'influxdb',
    ],
    classifiers=['Development Status :: 4 - Beta',
                 'Programming Language :: Python :: 3.2',
                 'Programming Language :: Python :: 3.3',
                 'Programming Language :: Python :: 3.4',
                 'Programming Language :: Python :: 3.5',
                 'License :: OSI Approved :: BSD License'],
    packages=['easyctp'],
)
