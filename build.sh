#!/bin/bash


CUR_DIR=$(pwd)
ZIP_DIR="${CUR_DIR}/zip"
OUTPUT_ZIP="${CUR_DIR}/ctracker.zip"
VENV="${ZIP_DIR}/${VENV}"
VENV_PATH="${VENV}/lib/python3.6/site-packages"

rm -f "${OUTPUT_ZIP}"
rm -Rf "${ZIP_DIR}"
mkdir -p "${VENV}"

deactivate
python -m venv "${VENV}"
source "${VENV}/bin/activate"

pip3 install -r requirements.txt
deactivate

pushd "${VENV_PATH}"
zip -r9 "${OUTPUT_ZIP}" .
popd
zip -gr "${OUTPUT_ZIP}" get_data.py
