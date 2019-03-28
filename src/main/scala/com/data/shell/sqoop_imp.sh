#!/bin/bash

opts=$@

get_char()
{
   SAVEDSTTY=`stty -g`
   stty -echo
   stty cbreak
   dd if=/dev/tty bs=1 count=1 2>/dev/null
   stty -raw
   stty echo
   stty $SAVEDSTTY
}
getparam(){
arg=$1
echo $opts | xargs -n1 |cut -b 2- | awk -F'=' '(if($1=="'"$arg"'") print $2)'

}

show_version()
{
        echo "version: 1.0"
        echo "updated date: 2019-01-09"
}

show_usage()
{
        echo -e "`printf %-16s "Usage: $0"` [-h|--help]"
        echo -e "`printf %-16s ` [-v|-V|--version]"
        echo -e "`printf %-16s ` [-H|--host ... ]"
        echo -e "`printf %-16s ` [-P|--port ... ]"
        echo -e "`printf %-16s ` [-u|--user ... ]"
        echo -e "`printf %-16s ` [-p|--password ... ]"
        echo -e "`printf %-16s ` [-D|--dstdb ... ]"
        echo -e "`printf %-16s ` [-d|--db]"
        echo -e "`printf %-16s ` [-i|--intab]"
        echo -e "`printf %-16s ` [-o|--outtab]"
        echo -e "`printf %-16s ` [-a|--address]"
        echo -e "`printf %-16s ` [-k|--keytab]"
        echo -e "`printf %-16s ` [-U|--user]"
        echo -e "`printf %-16s ` [-s|--source]"
        exit -1
}

ARGS=`getopt -o ":hvVH:P:u:p:D:d:i:o:a:k:U:s:" --long help,version,host:,port:,username:,password:,dstdb::,db:,intab:,outtab::,address::,keytab::,user::,source:  -n 'sqoop_imp.bash' -- "$@" 2>/dev/null`
[ $? != 0 ] && echo -e "\033[31mERROR: unknown argument! \033[0m\n" && show_usage && exit 1

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

echo ARGS=[ $ARGS ]
eval set -- "${ARGS}"
echo formatted parameters=[ $@ ]


while true
do
    [ -z "$1" ] && break;
    case "$1" in
        -h|--help)
        show_usage; exit 0
        ;;
        -v|-V|--version)
        show_version; exit 0
        ;;
        -H|--host)
        echo "host value: $2";
        HOST=$2
        shift 2
        ;;
        -P|--port)
        echo "port value: $2";
        PORT=$2
        shift 2
        ;;
        -u|--username)
        echo "username value: $2";
        USERNAME=$2
        shift 2
        ;;
        -p|--password)
        echo "password value: $2";
        PASSWORD=$2
        shift 2
        ;;
        -D|--dstdb)
        echo "dst database value: $2";
        DSTDB=$2
        shift 2
        ;;
        -d|--db)
        echo "database value: $2";
        DB=$2
        shift 2
        ;;
        -i|--intab)
        echo "input table value: $2"
        INTAB=$2
        shift 2
        ;;
         -o|--outab)
        echo "out table value: $2"
        OUTAB=$2
        shift 2
        ;;
        -a|--address)
        echo "address value: $2"
        ADDRESS=$2
        shift 2
        ;;
        -k|--keytab)
        echo "keytab value: $2"
        KEYTAB=$2
        shift 2
        ;;
        -U|--user)
        echo "user value: $2"
        USER=$2
        shift 2
        ;;
        -s|--source)
        echo "source value: $2"
        SOURCE=$2
        shift 2
        ;;
        ---)
        shift
        break
        ;;
        *)
        echo -e "\033[31mERROR: Unknow argument! \033[0m\n" && show_usage && exit 1
        ;;
     esac
done

####################################  main  #####################################################

if [ -z "$KEYTAB" -a -n $USER ];then
    KEYTAB="${USER}.keytab"
elif [ -n "${KEYTAB}" -a -n ${USER} ]; then
   KEYTAB=$KEYTAB
   echo $KEYTAB
else
   echo "keytab and user not found argument ! please input keytab and user argument"
   exit 1
fi

basepath=$(cd `dirname $0`; pwd)

if [ -n ${KEYTAB} ];then
   find_keytab=`su - ${USER} -c "cd ~ && find . -maxdepth 1 -type f -name "${KEYTAB}" 2> /dev/null"`
fi

if [ -z ${find_keytab} ];then
   echo "not found ${KEYTAB} file, is fail"
   key_flag=0
   exit 1
else
   echo "found ${KEYTAB} file, is ok"
   kinit_key=`su - ${USER} -c "kinit -kt ${KEYTAB} ${USER}"`
   key_flag=1
fi


if [ "${key_flag}"  -eq 1 ];then
   echo "kinit ${KEYTAB} is ok"
   result=1
else
   echo "kinit ${KEYTAB} is fail"
   result=0
   exit 1
fi


if [ "${result}" -eq 1 ];then
    echon "hive_show_db"
    hive_show_db=`su - ${USER} -c "hive -v -S -e \"use ${DSTDB}\""`
fi


if [ $? -eq 0 ];then
   echo "${DSTDB} database exists"
else
   echo "${DSTDB} database not exists, please create database!"
   exit 1
#   hive_create_db=`su - ${USER} -c "hive -v -S -e \"create database if not exists ${DSTDB}\""`
#   if [ $? -eq 0 ];then
#      echo "${DSTDB} create successful!"
#   else
#      echo "${DSTDB} create fail"
#   fi
fi

if [ "${SOURCE}"x = "oracle"x ];then
   ADDRESS="jdbc:oracle:thin:@${HOST}:${PORT}:${DB}"
elif [ "${SOURCE}"x = "mysql"x  ];then
   ADDRESS="jdbc:mysql://${HOST}:${PORT}/${DB}"
else
   echo "${SOURCE} Args ERROR! ";exit 0
fi


if [ -z "${OUTTAB}" ];then
   OUTTAB=$INTAB
fi


sqoop_all_imp=`
  su - ${USER}  sqoop import --connect $ADDRESS --username $USERNAME --password $PASSWORD \
  --fields-terminated-by '\t' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' \
  --query "select * from ${INTAB} where 1=1 and $CONDITIONS"  --target-dir /user/hive/warehouse/${OUTTAB} \
  --delete-target-dir -m 1 \
  --hive-import --hive-database ${DSTDB} \
  --hive-overwrite --hive-table ${OUTTAB}
`

sqoop_inc_imp=`
  su - ${USER}  sqoop import --connect $ADDRESS --username $USERNAME --password $PASSWORD \
  --fields-terminated-by '\t' --lines-terminated-by '\n' --null-string '\\N' --null-non-string '\\N' \
  --query "select * from ${INTAB} where 1=1 and $CONDITIONS" \
  --target-dir   /user/hive/warehouse/${OUTTAB} \
  -m 1 \
  --check-column "id" --incremental append --last-value ${MAX_ID}
  --hive-import --hive-database ${DSTDB} \
  --hive-overwrite --hive-table ${OUTTAB}
`
// 	  --check-column "id" --incremental append --last-value 5 \#增量导入同一个文件内

