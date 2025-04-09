#!/bin/bash

data_dir="./.data"
zip_file="the-movies-dataset.zip"
zip_path="${data_dir}/${zip_file}"

mkdir -p "${data_dir}"
curl -L -o "${zip_path}" https://www.kaggle.com/api/v1/datasets/download/rounakbanik/the-movies-dataset
if [ $? -eq 0 ]; then
  unzip -o "${zip_path}" -d "${data_dir}"
  if [ $? -eq 0 ]; then
    echo "Unziped datasets at '${data_dir}'."
  else
    echo "Error unziping file."
  fi
else
  echo "Error downloading file."
fi

exit 0