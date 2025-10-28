# DBLite

A lightweight Redis-like database in Python.

## Installation

pip install .

## Running Server

dblite-server --port 31337

## Usage

from dblite import Client
c = Client()
c.set('key', 'value')
print(c.get('key'))
