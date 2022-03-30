# packaging/docker
This directory contains the pieces for building FoundationDB docker images. 

`build-images.sh` will optionally take a single parameter that will be used as an
image tag postfix.

For more details it is best to read the `build-images.sh` shell script itself to
learn more about how the images are built. 

For details about what is in the images, peruse `Dockerfile{,.eks}`

the `samples` directory is out of date, and anything therein should be used with 
the expectation that it is, at least, partially (if not entirely) incorrect. 