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
    license='MIT',
    dependency_links=[
        'git+ssh://git@github.com:shimpeiws/sqs-to-thread-decorator.git#egg=queue_for_thread'],
)
