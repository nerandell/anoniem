from setuptools import setup, find_packages

setup(
    name='anoniem',
    version='0.0.1',
    author='Ankit Chandawala',
    author_email='ankitchandawala@gmail.com',
    url='https://github.com/nerandell/anoniem',
    description='High performance data anonymization tool',
    packages=find_packages(exclude=['tests']),
)