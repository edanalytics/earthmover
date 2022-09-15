# import os
import pathlib
import setuptools



# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# # automatically captured required modules for install_requires in requirements.txt
# with open(os.path.join(HERE, 'requirements.txt'), encoding='utf-8') as fp:
#     all_reqs = fp.read().split('\n')

# install_requires = [
#     x.strip()
#     for x in all_reqs
#     if 'git+' not in x and not x.startswith('#') and not x.startswith('-')
# ]
#
# dependency_links = [
#     x.strip().replace('git+', '')
#     for x in all_reqs
#     if 'git+' not in x
# ]

setuptools.setup (
    name = 'earthmover',
    description = 'Transforms tabular data sources into text-based data via YAML configuration',
    version = '0.0.1',
    packages = ['earthmover'], #setuptools.find_packages(), # list of all packages
    # install_requires = install_requires,
    python_requires='>=3',
    entry_points='''
        [console_scripts]
        earthmover=earthmover.__main__:main
    ''',
    author="Tom Reitz",
    keyword="data, transformation",
    long_description=README,
    long_description_content_type="text/markdown",
    license='Apache 2.0',
    url='https://github.com/edanalytics/earthmover',
    download_url='https://github.com/edanalytics/earthmover/archive/0.0.1.tar.gz',
    # dependency_links=dependency_links,
    author_email='treitz@edanalytics.org',
    classifiers=[
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