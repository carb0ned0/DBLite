from setuptools import setup

setup(
    name='dblite',
    version='0.1.0',
    py_modules=['dblite'],
    install_requires=['gevent'],
    entry_points={'console_scripts': ['dblite-server = dblite:main']},
)