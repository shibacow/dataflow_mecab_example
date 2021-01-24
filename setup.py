from setuptools import setup, find_packages
setup(
    name='mecab_test',
    version='0.0.1',
    install_requires=[
        'mecab-python3',
    ],
    description='mecab test',
    packages=find_packages(),
    long_description=open('README.md', 'rt').read()
)
