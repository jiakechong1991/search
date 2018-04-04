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

course_name=$data_dir/course_name
course_name_diff=$data_dir/course_name_diff
course_name_old=$old_data_dir/course_name

course_name_pinyin=$data_dir/course_name_pinyin
course_name_pinyin_diff=$data_dir/course_name_pinyin_diff
course_name_pinyin_old=$old_data_dir/course_name_pinyin

touch $course_name
touch $course_name_pinyin
cp $course_name $course_name_old
cp $course_name_pinyin $course_name_pinyin_old

python course/dump_course_name.py --file_ot $course_name 
python dump_pinyin_weight.py --file_in $course_name --file_ot $course_name_pinyin\
    --FULL_PINYIN --FIRST_LETTER --INITIAL --FUZZY_PINYIN

python diff_data.py --old_file $course_name_old --new_file $course_name --file_ot $course_name_diff
python diff_data.py --old_file $course_name_pinyin_old --new_file $course_name_pinyin --file_ot $course_name_pinyin_diff 

python upload_es.py --file_in $course_name_diff --doc_type course_name
python upload_es.py --file_in $course_name_pinyin_diff --doc_type course_name

