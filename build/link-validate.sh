#/bin/sh
#
# This script is used to validate the shared libraries

verlte() {
    [  "$1" = "`echo -e "$1\n$2" | sort -V | head -n1`" ]
}

ALLOWED_SHARED_LIBS=("libdl.so.2" "libpthread.so.0" "librt.so.1" "libm.so.6" "libc.so.6" "ld-linux-x86-64.so.2" "libfdb_c.so")

if [ "$#" -ne 2 ]; then	
    echo "USAGE: link-validate.sh BINNAME GLIBC_VERSION"
    exit 1
fi

# Step 1: glibc version

for i in $(objdump -T "$1" | awk '{print $5}' | grep GLIBC | sed 's/ *$//g' | sed 's/GLIBC_//' | sort | uniq); do
	if ! verlte "$i" "$2"; then 
		echo "!!! WARNING: DEPENDENCY ON NEWER LIBC DETECTED !!!"
		exit 1
	fi
done

# Step 2: Other dynamic dependencies

for j in $(objdump -p "$1" | grep NEEDED | awk '{print $2}'); do
	PRESENT=0
	for k in ${ALLOWED_SHARED_LIBS[@]}; do
		if [[ "$k" == "$j" ]]; then
			PRESENT=1
			break
		fi
	done
	if ! [[ $PRESENT == 1 ]]; then 
		echo "!!! WARNING: UNKNOWN SHARED OBJECT DEPENDENCY DETECTED: $j !!!"
		exit 1
	fi
done
