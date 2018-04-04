PROJ_PATH=".."
FIX_PATH="."
DIR_DATA_BASE=$PROJ_PATH/../data

# 配置文件
FIX_DEFINITION="definition.py.fix"
ONLINE_DEFINITION="definition.py.online"
DEFINITION="definition.py"

# 将definition文件copy到修复文件夹来
cp $PROJ_PATH/$DEFINITION $FIX_PATH/$ONLINE_DEFINITION
if [ ! -f "$PROJ_PATH/$FIX_DEFINITION" ]; then
    echo "$PROJ_PATH/$FIX_DEFINITION not exists!"
    exit -1
else
    cp $PROJ_PATH/$FIX_DEFINITION $FIX_PATH/$FIX_DEFINITON
fi

# 执行文件
FILE_INIT_ES="init_env.py"
cp $PROJ_PATH/$FILE_INIT_ES $FIX_PATH/$FILE_INIT_ES

# 数据文件
FILE_COURSE_DATA_OLD=${DIR_DATA_BASE}/course_name_old.data
FILE_COURSE_PINYIN_OLD=${DIR_DATA_BASE}/course_name_old.pinyin
FILE_USER_WEIGHT_OLD=${DIR_DATA_BASE}/user_old.data
FILE_USER_PINYIN_WEIGHT_OLD=${DIR_DATA_BASE}/user_old.pinyin
FILE_WORD_WEIGHT_OLD=${DIR_DATA_BASE}/main_old.data
FILE_PINYIN_WEIGHT_OLD=${DIR_DATA_BASE}/main_old.pinyin

# 建临时sug-index
mv $FIX_PATH/$FIX_DEFINITION $FIX_PATH/$DEFINITION  # fix -> .
if [ -f $FIX_PATH/definition.pyc ]; then
    rm $FIX_PATH/definition.pyc
fi
python build_mapping.py
python init_env.py 3 $FILE_COURSE_DATA_OLD
python init_env.py 3 $FILE_COURSE_PINYIN_OLD
python init_env.py 2 $FILE_USER_WEIGHT_OLD
python init_env.py 2 $FILE_USER_PINYIN_WEIGHT_OLD
python init_env.py 1 $FILE_WORD_WEIGHT_OLD
python init_env.py 1 $FILE_PINYIN_WEIGHT_OLD

# 将线上的索引切到临时的doc_type
if [ -f $PROJ_PATH/definition.pyc ]; then
    rm $PROJ_PATH/definition.pyc
fi
mv $PROJ_PATH/$DEFINITION $PROJ_PATH/$ONLINE_DEFINITION
mv $PROJ_PATH/$FIX_DEFINITION $PROJ_PATH/$DEFINITION

# 重建sug-index
mv $FIX_PATH/$DEFINITION $FIX_PATH/$FIX_DEFINITION
mv $FIX_PATH/$ONLINE_DEFINITION $FIX_PATH/$DEFINITION
if [ -f $FIX_PATH/definition.pyc ]; then
    rm $FIX_PATH/definition.pyc
fi
python build_mapping.py
python init_env.py 3 $FILE_COURSE_DATA_OLD
python init_env.py 3 $FILE_COURSE_PINYIN_OLD
python init_env.py 2 $FILE_USER_WEIGHT_OLD
python init_env.py 2 $FILE_USER_PINYIN_WEIGHT_OLD
python init_env.py 1 $FILE_WORD_WEIGHT_OLD
python init_env.py 1 $FILE_PINYIN_WEIGHT_OLD


# 将线上的索引切回原来的doc_type
if [ -f $PROJ_PATH/definition.pyc ]; then
    rm $PROJ_PATH/definition.pyc
fi
mv $PROJ_PATH/$DEFINITION $PROJ_PATH/$FIX_DEFINITION
mv $PROJ_PATH/$ONLINE_DEFINITION $PROJ_PATH/$DEFINITION

# 删掉临时索引
mv $FIX_PATH/$FIX_DEFINITION $FIX_PATH/$DEFINITION
if [ -f $FIX_PATH/definition.pyc ]; then
    rm $FIX_PATH/definition.pyc
fi
python delete_index.py

rm $FIX_PATH/definition*
rm $FIX_PATH/init_env.py
