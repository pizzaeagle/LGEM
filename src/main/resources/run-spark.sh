spark-submit \
 --properties-file $2 \
 --files $2 \
 --driver-java-options "-Dconfig.file=$2" \
 $1