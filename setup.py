from setuptools import setup, find_packages
import sys


install_requires = [
    'boto3',
    'docker',
]

sys.path.append('./test')

setup(
    name='spr_adbi',
    version='0.3.5',
    description='Sprocket ADBI Library',
    author='mokemokechicken',
    author_email='mokemokechicken@gmail.com',
    url='https://github.com/mokemokechicken/spr_adbi',
    install_requires=install_requires,
    py_modules=["spr_adbi"],
    packages=find_packages(exclude=["test*"]),
    test_suite='test',
    license="MIT",
)
