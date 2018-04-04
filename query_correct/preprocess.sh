#/bin/bash
set -u
set -x
set -e

work_dir="preprocess"
cd $work_dir

data_dir="./data"

if [ ! -d $data_dir ]; then
    mkdir $data_dir
fi

file_all_word="word.txt"
file_chinese_word="chinese_word.txt"
file_non_chinese_word="non_chinese_word.txt"
file_pinyin="pinyin.txt"
file_similar_word="similar_word.txt"

python dump_data.py --file_ot $data_dir/$file_all_word
python part_chinese.py --file_in $data_dir/$file_all_word --chinese_file_ot $data_dir/$file_chinese_word --non_chinese_file_ot $data_dir/$file_non_chinese_word
python word_weight_extender.py --category pinyin --file_in $data_dir/$file_chinese_word --file_ot $data_dir/$file_pinyin
python word_weight_extender.py --category similar_form --file_in $data_dir/$file_chinese_word --file_ot $data_dir/$file_similar_word

es_index="qc-index"
old_data_dir="./old_data"

python upload_es.py $file_chinese_word --doc_type chinese_ngram
python upload_es.py $file_similar_word --doc_type similar_form_no_ngram
python upload_es.py $file_non_chinese_word --doc_type non_chinese_ngram
python upload_es.py $file_pinyin --doc_type pinyin_ngram

# 把生成的文件从data复制到old_data 

if [ ! -d $old_data_dir ]; then
    mkdir $old_data_dir
fi

for filename in $file_all_word $file_chinese_word $file_non_chinese_word $file_pinyin $file_similar_word
do
    cp $data_dir/$filename $old_data_dir
done


