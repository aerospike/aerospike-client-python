#! /bin/bash

if [[ $TRAVIS_OS_NAME == "osx" ]]; then
    brew update
    brew install openssl
    brew install pyenv
    pyenv install $TRAVIS_PYTHON_VERSION
    pyenv shell $TRAVIS_PYTHON_VERSION
    pyenv local $TRAVIS_PYTHON_VERSION
    pyenv global $TRAVIS_PYTHON_VERSION
    pyenv rehash
fi
