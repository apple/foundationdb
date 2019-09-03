#!/bin/bash

set -e
OPTIONS=''

# Get compiler version and major version
COMPILER_VER=$("${CC}" -dumpversion)
COMPILER_MAJVER="${COMPILER_VER%%\.*}"

# Add linker, if specified and valid
# The linker to use for building:
# can be LD (system default, default choice), GOLD, LLD, or BFD
if [ -n "${USE_LD}" ] && \
	 (([[ "${CC}" == *"gcc"* ]] && [ "${COMPILER_MAJVER}" -ge 9 ]) || \
    ([[ "${CXX}" == *"clang++"* ]] && [ "${COMPILER_MAJVER}" -ge 4 ]) )
then
	if [ "${PLATFORM}" == "linux" ]; then
		if [ "${USE_LD}" == "BFD" ]; then
			OPTIONS+='-fuse-ld=bfd -Wl,--disable-new-dtags'
		elif [ "${USE_LD}" == "GOLD" ]; then
			OPTIONS+='-fuse-ld=gold -Wl,--disable-new-dtags'
		elif [ "${USE_LD}" == "LLD" ]; then
			OPTIONS+='-fuse-ld=lld -Wl,--disable-new-dtags'
		elif [ "${USE_LD}" != "DEFAULT" ] && [ "${USE_LD}" != "LD" ]; then
			echo 'USE_LD must be set to DEFAULT, LD, BFD, GOLD, or LLD!'
			exit 1
		fi
	fi
fi

case $1 in
    Application | DynamicLibrary)
	echo "Linking        $3"

	if [ "$1" = "DynamicLibrary" ]; then
		OPTIONS+=" -shared"
		if [ "$PLATFORM" = "linux" ]; then
			OPTIONS+=" -Wl,-z,noexecstack -Wl,-soname,$( basename $3 )"
		elif [ "$PLATFORM" = "osx" ]; then
			OPTIONS+=" -Wl,-dylib_install_name -Wl,$( basename $3 )"
		fi
	fi

	OPTIONS=$( eval echo "$OPTIONS $LDFLAGS \$$2_OBJECTS \$$2_LIBS \$$2_STATIC_LIBS_REAL \$$2_LDFLAGS -o $3" )

	if [[ "${OPTIONS}" == *"-static-libstdc++"* ]]; then
	  staticlibs=()
	  staticpaths=''
	  if [[ "${CC}" == *"gcc"* ]]; then
	    staticlibs+=('libstdc++.a')
	  elif [[ "${CXX}" == *"clang++"* ]]; then
	    staticlibs+=('libc++.a' 'libc++abi.a')
	  fi
    for staticlib in "${staticlibs[@]}"; do
	    staticpaths+="$("${CC}" -print-file-name="${staticlib}") "
	  done
	  OPTIONS=$( echo $OPTIONS | sed -e s,-static-libstdc\+\+,, -e s,\$,\ "${staticpaths}"\ -lm, )
	fi

	case $PLATFORM in
	    osx)
		if [[ "${OPTIONS}" == *"-static-libgcc"* ]]; then
		    $( $CC -### $OPTIONS 2>&1 | grep '^ ' | sed -e s,^\ ,, -e s,-lgcc[^\ ]*,,g -e s,\",,g -e s,\$,\ `$CC -print-file-name=libgcc_eh.a`, -e s,10.8.2,10.6, )
		else
		    $CC $OPTIONS
		fi
		;;
	    *)
		$CC $OPTIONS
		;;
	esac

	if [ -z "$UNSTRIPPED" ]; then
	    if [ -z "${NOSTRIP}" ]; then echo "Stripping      $3"; else echo "Not stripping  $3"; fi

	    case $1 in
		Application)
		    case $PLATFORM in
			linux)
			    objcopy --only-keep-debug $3 $3.debug
			    if [ -z "${NOSTRIP}" ]; then strip --strip-debug --strip-unneeded $3; fi
			    objcopy --add-gnu-debuglink=$3.debug $3
			    ./build/link-validate.sh $3 $4
			    ;;
			osx)
			    cp $3 $3.debug
			    if [ -z "${NOSTRIP}" ]; then strip $3; fi
			    ;;
			*)
			    echo "I don't know how to strip a binary on $PLATFORM"
			    exit 1
			    ;;
		    esac
		    ;;
		DynamicLibrary)
		    cp $3 $3-debug
		    case $PLATFORM in
			linux)
			    if [ -z "${NOSTRIP}" ]; then strip --strip-all $3; fi
			    ;;
			osx)
			    if [ -z "${NOSTRIP}" ]; then strip -S -x $3; fi
			    ;;
			*)
			    echo "I don't know how to strip a library on $PLATFORM"
			    exit 1
			    ;;
		    esac
		    ;;
	    esac
	fi
	;;
    StaticLibrary)
	echo "Archiving      $3"
	rm -f $3
	eval ar rcs $3 \$$2_OBJECTS
	;;
    *)
	echo "I don't know how to build a $1"
	exit 1
	;;
esac
