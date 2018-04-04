set -x
. ./main_env.sh

if [ $# -ne 1 ]; then
echo "USAGE: $0 mode(init/daily)"
exit 1
fi

set -u
set -e

mode=$1

data_dir="./data"
old_data_dir="./old_data"
if [ ! -d $data_dir ]; then
    mkdir $data_dir
fi

if [ ! -d $old_data_dir ]; then
    mkdir $old_data_dir
fi

main_word_num=$data_dir/main_word_num
old_main_word_num=$old_data_dir/main_word_num

main_text=$data_dir/main_text
main_unfilter=$data_dir/main_unfilter
main=$data_dir/main
main_diff=$data_dir/main_diff
main_old=$old_data_dir/main

main_pinyin=$data_dir/main_pinyin
main_pinyin_diff=$data_dir/main_pinyin_diff
main_pinyin_old=$old_data_dir/main_pinyin

touch $main
touch $main_pinyin
touch $main_word_num

cp $main $main_old
cp $main_pinyin $main_pinyin_old

python main/dump_main_db.py --file_ot $main_text 
python main/word_weight_getter.py --file_in $main_text --file_ot $main_unfilter

if [ $mode = "init" ]; then
    python main/offline_filter.py --file_in $main_unfilter --file_ot $main --file_word_num $main_word_num
    cp $main_word_num $old_main_word_num
    python main/online_filter.py --file_in $old_main_word_num --file_ot $main_word_num
else
    weekday=`date +%w`
    if [ $weekday -eq 2 ]; then
        cp $main_word_num $old_main_word_num
        python main/online_filter.py --file_in $old_main_word_num --file_ot $main_word_num
    fi
fi

python main/offline_filter.py --file_in $main_unfilter --file_ot $main --file_word_num $main_word_num

python dump_pinyin_weight.py --file_in $main --file_ot $main_pinyin\
    --FULL_PINYIN --FIRST_LETTER --INITIAL --FUZZY_PINYIN --MIX_PINYIN_WITH_CHINESE

python diff_data.py --old_file $main_old --new_file $main --file_ot $main_diff
python diff_data.py --old_file $main_pinyin_old --new_file $main_pinyin --file_ot $main_pinyin_diff 

python upload_es.py --file_in $main_diff --doc_type platform
python upload_es.py --file_in $main_pinyin_diff --doc_type platform
