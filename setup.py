from setuptools import find_packages, setup
setup(
    name='l_ozdm',
    packages=find_packages(),
    version='0.0.3',
    description='OpenZDM Library Functions and Classes',
    author='OpenZDM',
    license='OpenZDM',
    install_requires=['avro','python-qpid-proton']
)
