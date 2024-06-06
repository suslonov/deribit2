#!/usr/bin/env bash
pushd "$(dirname "${BASH_SOURCE[0]}")"
echo "clean build folders"
rm -fr build
rm -fr dist
rm -fr *.egg-info
echo "Script executing from: ${PWD}"
python setup.py bdist_wheel
popd
