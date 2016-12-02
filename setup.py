from setuptools import setup, find_packages

setup(
    name='elastic-benchmark',
    version='0.0.1',
    description='Parses a given input and inserts into ElasticSearch.',
    author='Stephen Lowrie',
    author_email='stephen.lowrie@rackspace.com',
    url='https://github.com/osic/elastic-benchmark',
    packages=['elastic_benchmark'],
    install_requires=open('requirements.txt').read(),
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: Other/Proprietary License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ),
    entry_points={
        'console_scripts': [
            'elastic-benchmark = elastic_benchmark.main:entry_point',
            'elastic-upgrade = elastic_benchmark.upgrade:entry_point']})
