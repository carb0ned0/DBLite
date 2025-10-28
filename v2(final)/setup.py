from setuptools import setup

setup(
    name='dblite',
    version='0.1.0',
    py_modules=['dblite'],
    install_requires=[],
    extras_require={'gevent': ['gevent']},
    entry_points={'console_scripts': ['dblite-server = dblite:main']},
)