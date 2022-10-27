import os
import pathlib
import setuptools

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# The current version
VERSION = (HERE / "VERSION.txt").read_text()

# automatically captured required modules for install_requires in requirements.txt
with open(os.path.join(HERE, 'requirements.txt'), encoding='utf-8') as fp:
    all_reqs = fp.readlines()

setuptools.setup (
    name = 'earthmover',
    description = 'Transforms tabular data sources into text-based data via YAML configuration',
    version = VERSION,
    packages = setuptools.find_namespace_packages(include=['earthmover', 'earthmover.*']),
    include_package_data=True,
    install_requires = all_reqs,
    python_requires = '>=3',
    entry_points = '''
        [console_scripts]
        earthmover=earthmover.__main__:main
    ''',
    author = "Tom Reitz, Jay Kaiser",
    keyword = "data, transformation",
    long_description = README,
    long_description_content_type = "text/markdown",
    license = 'Apache 2.0',
    url = 'https://github.com/edanalytics/earthmover',
    download_url = 'https://github.com/edanalytics/earthmover/archive/refs/tags/v0.0.4.tar.gz',
    dependency_links = all_reqs,
    author_email = 'treitz@edanalytics.org, jkaiser@edanalytics.org',
    classifiers = [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Office/Business",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities"
    ]
)