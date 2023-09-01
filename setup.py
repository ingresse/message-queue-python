import codecs
import re
import os

from setuptools import setup, find_packages


def read(*parts):
    path = os.path.join(os.path.dirname(__file__), *parts)

    with codecs.open(path, encoding='utf-8') as fobj:
        return fobj.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)

    if version_match:
        return version_match.group(1)

    raise RuntimeError('Unable to find version string.')

VERSION = find_version('message_queue', '__init__.py')

setup(
    name='message-queue',
    version=VERSION,
    description='Message Queue',
    long_description='Message Queue python library to publish and subscribe to queues with diferent types of adapters.',
    url='https://github.com/ingresse/message-queue-python',
    author='Ingresse',
    author_email='vitor.leal@ingresse.com',
    license='BSD',
    packages=find_packages(),
    install_requires=['pika==0.13.1'],
    download_url='https://github.com/ingresse/message-queue-python/tarball/%r'.format(VERSION),
    keywords='message queue rabbit amqp pub/sub',
)

