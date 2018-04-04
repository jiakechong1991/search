file_path=$(pwd)/$0
preprocess_dir=$(dirname $file_path)
sug_dir=$(dirname $preprocess_dir)
search_dir=$(dirname $sug_dir)

export PYTHONPATH=$PYTHONPATH:$sug_dir:$search_dir
