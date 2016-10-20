#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

# AC_PREREQ(2.65)
AC_INIT([tagsistant],[0.8.2],[tx0@strumentiresistenti.org])
# AC_CANONICAL_TARGET
AM_INIT_AUTOMAKE([foreign subdir-objects])
AC_COPYRIGHT([tagsistant is (C) Tx0 <tx0@strumentiresistenti.org>])
AC_CONFIG_SRCDIR([src/tagsistant.c])
AC_CONFIG_FILES([Makefile src/Makefile src/plugins/Makefile])
AC_CONFIG_HEADER([config.h])
AM_PROG_AR

# AC_PROG_RANLIB

# Checks for programs.
AC_PROG_CC

AC_PROG_CC_C_O

LT_INIT

# Checks for libraries.

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h netinet/in.h stddef.h stdlib.h string.h sys/socket.h sys/statfs.h sys/statvfs.h sys/vfs.h syslog.h unistd.h utime.h sys/xattr.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_UID_T
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_CHECK_MEMBERS([struct stat.st_blksize])
AC_STRUCT_ST_BLOCKS
AC_CHECK_MEMBERS([struct stat.st_rdev])

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC

AC_FUNC_STAT
AC_FUNC_UTIME_NULL
AC_CHECK_FUNCS([inet_ntoa lchown mempcpy memset mkdir rmdir setenv socket strdup strerror utime])

dnl AC_SUBST([CFLAGS],["${CFLAGS} -g -O0 -D_FILE_OFFSET_BITS=64 -Wall -W"])
AC_SUBST([CFLAGS],["${CFLAGS} -D_FILE_OFFSET_BITS=64 -Wall -W"])

AC_CONFIG_MACRO_DIR([m4])

# add option for debugging on console instead of through syslog facility
debug_stderr=no
AC_ARG_ENABLE(
	[debug-stderr],
	[AS_HELP_STRING([--enable-debug-stderr], [print debugging statements on stderr instead of using syslog @<:@default=syslog@:>@])],
	[debug_stderr=yes]
)
if test [ "x$debug_stderr" == "xyes" ]; then
	AC_SUBST([CFLAGS],["-D_DEBUG_STDERR ${CFLAGS}"])
else
	AC_SUBST([CFLAGS],["-D_DEBUG_SYSLOG ${CFLAGS}"])
fi

# add option for debugging calls to free()
debug_free=no
AC_ARG_ENABLE(
	[debug-free],
	[AS_HELP_STRING([--enable-debug-free], [double check before free()ing a symbol @<:@default=no@:>@])],
	[debug_free=yes]
)
if test [ "x$debug_free" == "xyes" ]; then
	AC_SUBST([CFLAGS],["-D_DEBUG_FREE_CALLS ${CFLAGS}"])
fi

AC_CHECK_LIB([gthread-2.0], [g_thread_init],,[
    AC_MSG_FAILURE(["GThread support not available. Please install glib"])
])

AC_CHECK_LIB([glib-2.0], [g_main_loop_new],,[
    AC_MSG_FAILURE(["GLib support not available. Please install glib"])
])

AC_CHECK_LIB([gio-2.0], [g_file_new_for_path],,[
    AC_MSG_FAILURE(["GIO support not available. Please install glib"])
])

AC_CHECK_LIB([fuse],[fuse_main],,[
	echo "FUSE (Filesystem in USErspace) it's a great user space library"
	echo "which allows linux kernel users to create filesystem implementations"
	echo "without playing with complex kernel internals. tagsistant is entirely based"
	echo "on FUSE. Please install libfuse-dev package (or the one which best"
	echo "fits your linux distribution) or download and compile sources at:"
	echo "More info at http://fuse.sourceforge.net/"
	AC_MSG_FAILURE(["fuse not found"])
])

AC_CHECK_LIB([dbi],[dbi_conn_new],,[
	echo "DBI is an generic database abstactio layer used to connect to MySQL"
	echo "and will probably used to connect to SQLite too in future releases"
	echo "More info at http://libdbi.sourceforge.net/"
	AC_MSG_FAILURE(["libdbi not found"])
])

AC_CHECK_LIB([extractor],[EXTRACTOR_loadDefaultLibraries],[use_libextractor='0.5'],
	[AC_CHECK_LIB([extractor],[EXTRACTOR_extract],[use_libextractor='0.6'],[
		use_libextractor=0
		echo "libextractor is a universal metadata extractor. Tagsistant uses it"
		echo "to autotag your files."
		echo "More info at http://http://www.gnu.org/software/libextractor"
		AC_MSG_FAILURE(["libextractor not found"])
	])
])

echo $host .. $target
case $host in
	*-*-linux*)
		echo "Live long and prosper, GNU/Linux"
		;;
	*-*-darwin*)
		AC_SUBST([CFLAGS],["${CFLAGS} -D__FreeBSD__=10 -DMACOSX -DFUSE_USE_VERSION=25"])
		AC_SUBST([LDFLAGS],["${LDFLAGS} -flat_namespace -force_flat_namespace"])
		;;
esac

AC_OUTPUT

