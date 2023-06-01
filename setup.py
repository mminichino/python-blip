from setuptools import setup

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='pyblip',
    version='0.1.1',
    packages=['pyblip'],
    url='https://github.com/mminichino/python-blip',
    license='MIT License',
    author='Michael Minichino',
    python_requires='>=3.9',
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
    long_description_content_type='text/markdown'
)
