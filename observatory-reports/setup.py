from setuptools import setup, find_namespace_packages

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setup(
    name='observatory-reports',
    version='20.10.0',
    description='The Observatory Reports',
    license='Apache License Version 2.0',
    author='Curtin University',
    author_email='agent@observatory.academy',
    url='https://github.com/The-Academic-Observatory/observatory-platform',
    packages=find_namespace_packages(include=['observatory.*']),
    keywords=['science', 'data', 'workflows', 'academic institutes', 'observatory-reports'],
    install_requires=install_requires,
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities"
    ],
    python_requires='>=3.7'
)
