#!/bin/sh

today=`date +%Y%m%d`
sed -i -e "s/#define PACKAGE_STRING .*/#define PACKAGE_STRING \"tagsistant 0.3.$today\"/" config.h
sed -i -e "s/#define PACKAGE_VERSION .*/#define PACKAGE_VERSION \"0.3.$today\"" config.h
sed -i -e "s/#define VERSION .*/#define VERSION \"0.3.$today\"" config.h

dir="../tagsistant-0.4-$today"
mkdir $dir
tar cf - AUTHORS ChangeLog config.guess config.h config.sub configure COPYING depcomp INSTALL install-sh libtool Makefile missing mkinstalldirs NEWS README  TODO Makefile.am Makefile.in config.h.in ltmain.sh configure.ac m4/ src/*.c src/*.h src/test_suite.pl src/compat/ src/Makefile* | (cd $dir; tar xvf  -)
find $dir -name ".svn" | xargs rm -rf
