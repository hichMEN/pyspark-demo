from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

setup(
    name='PySpark-learn',
    version='0.0.1',
    description='learn how to use spark with python',
    long_description= readme,
    author='hichMEN',
    packages=find_packages(exclude=('tests', 'docs')),
    dependecy_links=[]
)