from setuptools import setup, find_packages

VERSION = '0.0.1'

setup(
    name='coffee_boat',
    version=VERSION,
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    packages=find_packages(),
    url='https://github.com/nteract/coffee_boat',
    license='LICENSE',
    description='Improve dependency management for PySpark in notebooks',
    long_description=open('README.md').read(),
    install_requires=[
        # We skip Pyspark as a requirement since many cloud notebooks won't have
        # this listed as provided (for now). TODO: Put back in once this isn't the case. #5
        #'pyspark>=2.2.0',
    ],
    tests_require=[
        'nose==1.3.7',
        'coverage>3.7.0',
        'unittest2>=1.0.0',
        'backports.weakref',
    ],
)
