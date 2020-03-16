#!/bin/bash

ZIP_DIR=./zip
OUTPUT_ZIP="${ZIP_DIR}/ctracker.zip"
VENV="${ZIP_DIR}/${VENV}"
VENV_PATH="${VENV}/lib/python3.6/site-packages"

rm -Rf "${ZIP_DIR}"
mkdir -p "${VENV}"

deactivate
python -m venv "${VENV}"
source "${VENV}/bin/activate"

pip3 install -r requirements.txt
deactivate

pushd "${VENV_PATH}"
zip -r9 "${OUTPUT_ZIP}"
popd
zip -gr "${OUTPUT_ZIP}" .
