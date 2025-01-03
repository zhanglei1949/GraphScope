CUR_DIR=$(dirname $0)
# ABS_CUR_DIR=$(realpath $CUR_DIR)
POSTGRES_ORIGIN_DIR=${CUR_DIR}/../third_party/postgres/src
POSTGRES_INCLUDE_DIR=${CUR_DIR}/../third_party/postgres/src/include
POSTGRES_DEST_DIR=${CUR_DIR}

# Delete all .c files in the destination directory
find $POSTGRES_DEST_DIR -name "*.c" -type f -delete

# cp -r $POSTGRES_ORIGIN_DIR/backend/* $POSTGRES_DEST_DIR/backend
# cp -r $POSTGRES_ORIGIN_DIR/timezone/* $POSTGRES_DEST_DIR/timezone
ORIGIN_BACKEND_DIR=${POSTGRES_ORIGIN_DIR}/backend
DEST_BACKEND_DIR=${POSTGRES_DEST_DIR}/backend

# get all directory path under $ORIGIN_BACKEND_DIR
#ALL_BACKEND_FILES=$(ls $ORIGIN_BACKEND_DIR/**/*.c)
ALL_BACKEND_BOOTSTRAP_FILES=$(find $ORIGIN_BACKEND_DIR/bootstrap -name "*.c")
ALL_BACKEND_ARCHIVE_FILES=$(find $ORIGIN_BACKEND_DIR/archive -name "*.c")
ALL_BACKEND_FORIEGN_FILES=$(find $ORIGIN_BACKEND_DIR/foreign -name "*.c")
ALL_BACKEND_ACCESS_FILES=$(find $ORIGIN_BACKEND_DIR/access -name "*.c")
ALL_BACKEND_BACKUP_FILES=$(find $ORIGIN_BACKEND_DIR/backup -name "*.c")
ALL_BACKEND_CATALOG_FILES=$(find $ORIGIN_BACKEND_DIR/catalog -name "*.c")
ALL_BACKEND_COMMAND_FILES=$(find $ORIGIN_BACKEND_DIR/commands -name "*.c")
ALL_BACKEND_EXECUTOR_FILES=$(find $ORIGIN_BACKEND_DIR/executor -name "*.c")
ALL_BACKEND_JIT_FILES=$(find $ORIGIN_BACKEND_DIR/jit -name "*.c")
ALL_BACKEND_LIB_FILES=$(find $ORIGIN_BACKEND_DIR/lib -name "*.c")
ALL_BACKEND_FILES_LIBPQ_FILES=$(find $ORIGIN_BACKEND_DIR/libpq -name "*.c")
ALL_BACKEND_NODES_FILES=$(find $ORIGIN_BACKEND_DIR/nodes -name "*.c")
ALL_BACKEND_OPTIMIZER_FILES=$(find $ORIGIN_BACKEND_DIR/optimizer -name "*.c")
ALL_BACKEND_PARSER_FILES=$(find $ORIGIN_BACKEND_DIR/parser -name "*.c")
ALL_BACKEND_PARTITIONING_FILES=$(find $ORIGIN_BACKEND_DIR/partitioning -name "*.c")
ALL_BACKEND_PORT_FILES=$(find $ORIGIN_BACKEND_DIR/port -name "*.c")
ALL_BACKEND_POSTMASTER_FILES=$(find $ORIGIN_BACKEND_DIR/postmaster -name "*.c")
ALL_BACKEND_REGEXP_FILES=$(find $ORIGIN_BACKEND_DIR/regex -name "*.c")
ALL_BACKEND_REPLICATION_FILES=$(find $ORIGIN_BACKEND_DIR/replication -name "*.c")
ALL_BACKEND_REWRITE_FILES=$(find $ORIGIN_BACKEND_DIR/rewrite -name "*.c")
ALL_BACKEND_SNOWBALL_FILES=$(find $ORIGIN_BACKEND_DIR/snowball -name "*.c")
ALL_BACKEND_STATISTICS_FILES=$(find $ORIGIN_BACKEND_DIR/statistics -name "*.c")
ALL_BACKEND_STORAGE_FILES=$(find $ORIGIN_BACKEND_DIR/storage -name "*.c")
ALL_BACKEND_TCOP_FILES=$(find $ORIGIN_BACKEND_DIR/tcop -name "*.c")
ALL_BACKEND_TSEARCH_FILES=$(find $ORIGIN_BACKEND_DIR/tsearch -name "*.c")
ALL_BACKEND_UTILS_FILES=$(find $ORIGIN_BACKEND_DIR/utils -name "*.c")


#### Copy headers ####
cp ${ORIGIN_BACKEND_DIR}/parser/gram.h ${DEST_BACKEND_DIR}/parser
cp ${POSTGRES_INCLUDE_DIR}/snowball/libstemmer/header.h ${DEST_BACKEND_DIR}/snowball/libstemmer
cp ${POSTGRES_INCLUDE_DIR}/snowball/libstemmer/api.h ${DEST_BACKEND_DIR}/snowball/libstemmer
cp ${ORIGIN_BACKEND_DIR}/replication/repl_gram.h ${DEST_BACKEND_DIR}/replication
cp ${ORIGIN_BACKEND_DIR}/replication/syncrep_gram.h ${DEST_BACKEND_DIR}/replication
cp ${ORIGIN_BACKEND_DIR}/utils/activity/wait_event_types.h ${DEST_BACKEND_DIR}/utils/activity
cp ${ORIGIN_BACKEND_DIR}/utils/adt/jsonpath_gram.h ${DEST_BACKEND_DIR}/utils/adt
cp ${ORIGIN_BACKEND_DIR}/bootstrap/bootparse.h ${DEST_BACKEND_DIR}/bootstrap



# concatenate all the files
ALL_BACKEND_FILES="${ALL_BACKEND_ARCHIVE_FILES} ${ALL_BACKEND_BOOTSTRAP_FILES} ${ALL_BACKEND_FORIEGN_FILES} ${ALL_BACKEND_ACCESS_FILES} ${ALL_BACKEND_BACKUP_FILES} ${ALL_BACKEND_CATALOG_FILES} ${ALL_BACKEND_COMMAND_FILES} ${ALL_BACKEND_EXECUTOR_FILES} ${ALL_BACKEND_JIT_FILES} ${ALL_BACKEND_LIB_FILES} ${ALL_BACKEND_FILES_LIBPQ_FILES} ${ALL_BACKEND_NODES_FILES} ${ALL_BACKEND_OPTIMIZER_FILES} ${ALL_BACKEND_PARSER_FILES} ${ALL_BACKEND_PARTITIONING_FILES} ${ALL_BACKEND_PORT_FILES} ${ALL_BACKEND_POSTMASTER_FILES} ${ALL_BACKEND_REGEXP_FILES} ${ALL_BACKEND_REPLICATION_FILES} ${ALL_BACKEND_REWRITE_FILES} ${ALL_BACKEND_SNOWBALL_FILES} ${ALL_BACKEND_STATISTICS_FILES} ${ALL_BACKEND_STORAGE_FILES} ${ALL_BACKEND_TCOP_FILES} ${ALL_BACKEND_TSEARCH_FILES} ${ALL_BACKEND_UTILS_FILES}"
for file_path in $ALL_BACKEND_FILES;
do
    # only keep the paths after '/src/backend'. i.e. for ./../third_party/postgres/src/backend/tsearch/regis.c, we only keep tsearch/regis.c
    rel_file_path=${file_path#"$ORIGIN_BACKEND_DIR/"}
    dir_name=$(dirname $rel_file_path)

    file_name=$(basename $file_path)
    mkdir -p $DEST_BACKEND_DIR/$dir_name
    if [ $file_name == "main.c" ] || [ $file_name == "launch_backend.c" ]; then
        echo "skip $file_path"
        continue
    fi
    
    #if file is pg_sema.c or pg_shmem.c, they are soft links, so skip them
    if [ $file_name == "pg_sema.c" ] || [ $file_name == "pg_shmem.c" ]; then
        echo "skip $file_path"
        continue
    fi
    echo "copy $file_path to $DEST_BACKEND_DIR/$dir_name"
    cp $file_path $DEST_BACKEND_DIR/$dir_name
done

# if symbolic link exists, remove it
if [ -L $DEST_BACKEND_DIR/port/pg_sema.c ]; then
    echo "remove $DEST_BACKEND_DIR/port/pg_sema.c"
    rm $DEST_BACKEND_DIR/port/pg_sema.c
fi
if [ -L $DEST_BACKEND_DIR/port/pg_shmem.c ]; then
    echo "remove $DEST_BACKEND_DIR/port/pg_shmem.c"
    rm $DEST_BACKEND_DIR/port/pg_shmem.c
fi
ln -s $DEST_BACKEND_DIR/port/posix_sema.c $DEST_BACKEND_DIR/port/pg_sema.c
ln -s $DEST_BACKEND_DIR/port/sysv_shmem.c $DEST_BACKEND_DIR/port/pg_shmem.c

TIMEZONE_ORIGIN_DIR=${POSTGRES_ORIGIN_DIR}/timezone
TIMEZONE_DEST_DIR=${POSTGRES_DEST_DIR}/timezone
cp -r $TIMEZONE_ORIGIN_DIR/*.c $TIMEZONE_DEST_DIR
mkdir -p $TIMEZONE_DEST_DIR/data
cp -r $TIMEZONE_ORIGIN_DIR/data/* $TIMEZONE_DEST_DIR/data
mkdir -p $TIMEZONE_DEST_DIR/tznames
cp -r $TIMEZONE_ORIGIN_DIR/tznames/* $TIMEZONE_DEST_DIR/tznames
cp $TIMEZONE_ORIGIN_DIR/*.txt $TIMEZONE_DEST_DIR



########## Postprocess ##########
# add #include <stdlib.h> to $DEST_BACKEND_DIR/snowball/libstemmer/api.c
echo "#include <stdlib.h>" | cat - $DEST_BACKEND_DIR/snowball/libstemmer/api.c > temp && mv temp $DEST_BACKEND_DIR/snowball/libstemmer/api.c

# echo 
#undef HAVE_SYNC_FILE_RANGE
#undef HAVE_SYNCFS
# to $DEST_BACKEND_DIR/storage/file/fd.c line 104
sed -i '104i #undef HAVE_SYNC_FILE_RANGE' $DEST_BACKEND_DIR/storage/file/fd.c
sed -i '105i #undef HAVE_SYNCFS' $DEST_BACKEND_DIR/storage/file/fd.c


# add <string.h> to ${DEST_BACKEND_DIR}/snowball/libstemmer/utilities.c
echo "#include <string.h>" | cat - ${DEST_BACKEND_DIR}/snowball/libstemmer/utilities.c > temp && mv temp ${DEST_BACKEND_DIR}/snowball/libstemmer/utilities.c

# add "c.h" to utils/activity/pgstat_wait_even.c
sed -i '18i #include "c.h"' ${DEST_BACKEND_DIR}/utils/activity/pgstat_wait_event.c
sed -i '19i #include "wait_event_types.h"' ${DEST_BACKEND_DIR}/utils/activity/pgstat_wait_event.c

#mv ${DEST_BACKEND_DIR}/utils/activity/wait_event_funcs_data.c ${DEST_BACKEND_DIR}/utils/activity/wait_event_funcs_data.h

sed -i '26i #include <stdbool.h>' ${DEST_BACKEND_DIR}/utils/adt/levenshtein.c
sed -i '27i #include <stddef.h>' ${DEST_BACKEND_DIR}/utils/adt/levenshtein.c
sed -i '28i #include "c.h"' ${DEST_BACKEND_DIR}/utils/adt/levenshtein.c
sed -i '29i #include "utils/elog.h"' ${DEST_BACKEND_DIR}/utils/adt/levenshtein.c