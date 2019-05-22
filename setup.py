from setuptools import setup


def readme():
    try:
        with open('README.rst') as f:
            return f.read()
    except FileNotFoundError:
        pass


setup(
    name='stackexchangeparser',
    version='1.5',
    description="A Prodigy compliant parser and API caller for the StackExchange corpus",
    long_description=readme(),
    keywords='Prodigy StackExchange StackOverflow NLP Natural Language Processing Parsing',
    packages=['separser', 'separser.utils'],
    url='http://github.com/clrogers2/stackexchangeparser',
    license='MIT',
    author='Christopher Rogers',
    author_email='christopher.rogers27@gmail.com',
    python_requires='>3.5',
    install_requires=['beautifulsoup4',
                      'requests',
                      'plac',
                      'lxml'
                      ],
    entry_points={
        'console_scripts': ['separse=separser.utils.command_line:main']
    },
    include_package_data=True,
    classifiers=["Development Status :: 3 - Alpha",
                 "Topic :: Utilities",
                 "Topic :: Documentation :: Sphinx",
                 "Topic :: Text Processing :: Markup :: XML"
                 "Environment :: Console",
                 "Programming Language :: Python :: 3 :: Only",
                 "Programming Language :: Python :: 3.5",
                 "Programming Language :: Python :: 3.6",
                 "Programming Language :: Python :: 3.7",
                 "Operating System :: POSIX :: Linux",
                 "Operating System :: Microsoft :: Windows :: Windows 10",
                 "Natural Language :: English",
                 ],
    zip_safe=True
)