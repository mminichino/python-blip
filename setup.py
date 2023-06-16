from setuptools import setup

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='pythonblip',
    version='0.3.1',
    packages=['pythonblip'],
    url='https://github.com/mminichino/python-blip',
    license='Apache License 2.0',
    author='Michael Minichino',
    python_requires='>=3.9',
    scripts=['bin/blipctl'],
    install_requires=[
        'attrs',
        'dnspython',
        'docker',
        'pytest',
        'requests',
        'urllib3',
        'websockets'
    ],
    author_email='info@unix.us.com',
    description='Couchbase BLIP Protocol Library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=["couchbase", "blip", "mobile", "syncgateway"],
    classifiers=[
          "Development Status :: 4 - Beta",
          "License :: OSI Approved :: Apache Software License",
          "Intended Audience :: Developers",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Programming Language :: Python :: 3",
          "Topic :: Database",
          "Topic :: System :: Networking",
          "Topic :: Software Development :: Libraries",
          "Topic :: Software Development :: Libraries :: Python Modules"],
)
