from setuptools import setup


setup(
    name='hh-deep-deep',
    url='https://github.com/TeamHG-Memex/hh-deep-deep',
    packages=['hh_deep_deep'],
    include_package_data=True,
    install_requires=[
        'pykafka==2.6.0',
        'tldextract',
    ],
    entry_points = {
        'console_scripts': [
            'hh-deep-deep-service=hh_deep_deep.service:main',
        ],
    },
    classifiers=[
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
)
