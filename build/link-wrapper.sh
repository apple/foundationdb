#!/bin/bash

set -e

case $1 in
    Application | DynamicLibrary)
	echo "Linking        $3"

	if [ "$1" = "DynamicLibrary" ]; then
	    OPTIONS="-shared"
	    if [ "$PLATFORM" = "linux" ]; then
		OPTIONS="$OPTIONS -Wl,-z,noexecstack -Wl,-soname,$( basename $3 )"
	    fi
	    if [ "$PLATFORM" = "osx" ]; then
		OPTIONS="$OPTIONS -Wl,-dylib_install_name -Wl,$( basename $3 )"
	    fi
	else
	    OPTIONS=
	fi

	OPTIONS=$( eval echo "$OPTIONS $LDFLAGS \$$2_LDFLAGS \$$2_OBJECTS \$$2_LIBS \$$2_STATIC_LIBS_REAL -o $3" )

	if echo $OPTIONS | grep -q -- -static-libstdc\+\+ ; then
	    OPTIONS=$( echo $OPTIONS | sed -e s,-static-libstdc\+\+,, -e s,\$,\ `$CC -print-file-name=libstdc++.a`\ -lm, )
	fi

	case $PLATFORM in
	    osx)
		if echo $OPTIONS | grep -q -- -static-libgcc ; then
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
