set -x
. ./env.sh

set -u
set -e

data_dir="./data"
old_data_dir="./old_data"
if [ ! -d $data_dir ]; then
    mkdir $data_dir
fi

if [ ! -d $old_data_dir ]; then
    mkdir $old_data_dir
fi

user_name=$data_dir/user_name
user_name_diff=$data_dir/user_name_diff
user_name_old=$old_data_dir/user_name

user_name_pinyin=$data_dir/user_name_pinyin
user_name_pinyin_diff=$data_dir/user_name_pinyin_diff
user_name_pinyin_old=$old_data_dir/user_name_pinyin

touch $user_name
touch $user_name_pinyin
cp $user_name $user_name_old
cp $user_name_pinyin $user_name_pinyin_old

python user/dump_user_name.py --file_ot $user_name
python dump_pinyin_weight.py --file_in $user_name --file_ot $user_name_pinyin\
    --FULL_PINYIN --FIRST_LETTER

python diff_data.py --old_file $user_name_old --new_file $user_name --file_ot $user_name_diff
python diff_data.py --old_file $user_name_pinyin_old --new_file $user_name_pinyin --file_ot $user_name_pinyin_diff

python upload_es.py --file_in $user_name_diff --doc_type forum_user
python upload_es.py --file_in $user_name_pinyin_diff --doc_type forum_user

