from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

setup(
    name='queue_for_thread',
    version='0.1.0',
    description='Queue For Thread',
    long_description=readme,
    author='Shimpei Takamatsu',
    author_email='shimpeiws@gmail.com',
    url='https://github.com/kennethreitz/samplemod',
    install_requires=['boto3'],
    license='MIT',
)
