SUBDIRS = plugins

bin_PROGRAMS = tagsistant

tagsistant_SOURCES = tagsistant.c\
	tagsistant.h\
	path_resolution.c\
	path_resolution.h\
	reasoner.c\
	debug.h\
	sql.c\
	sql.h\
	utils.c\
	plugin.c\
	plugin.h\
	deduplication.c\
	buildnumber.h\
	fuse_operations/operations.h\
	fuse_operations/access.c\
	fuse_operations/chmod.c\
	fuse_operations/chown.c\
	fuse_operations/getattr.c\
	fuse_operations/link.c\
	fuse_operations/mkdir.c\
	fuse_operations/mknod.c\
	fuse_operations/open.c\
	fuse_operations/read.c\
	fuse_operations/readdir.c\
	fuse_operations/readlink.c\
	fuse_operations/rename.c\
	fuse_operations/rmdir.c\
	fuse_operations/statfs.c\
	fuse_operations/statvfs.c\
	fuse_operations/symlink.c\
	fuse_operations/truncate.c\
	fuse_operations/unlink.c\
	fuse_operations/utime.c\
	fuse_operations/write.c\
	fuse_operations/flush.c\
	fuse_operations/release.c\
	fuse_operations/setxattr.c\
	fuse_operations/getxattr.c\
	fuse_operations/listxattr.c\
	fuse_operations/removexattr.c

tagsistant_CFLAGS = -D'PLUGINS_DIR="@libdir@/"' $(CFLAGS) `pkg-config --cflags glib-2.0`
tagsistant_LDFLAGS = -lfuse -ldl -rdynamic `pkg-config --libs glib-2.0` -lextractor
