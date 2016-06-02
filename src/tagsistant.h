/*
   Tagsistant (tagfs) -- tagsistant.h
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.
   Header file

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA. 
*/

/** Tagsistant codename of current release */
#define TAGSISTANT_CODENAME "Athens"

/** Tagsistant plugin prefix */
#define TAGSISTANT_PLUGIN_PREFIX "libtagsistant_"

/** Query delimiter as a string */
#define TAGSISTANT_QUERY_DELIMITER "@"

/** Query delimiter as a single char */
#define TAGSISTANT_QUERY_DELIMITER_CHAR '@'

/** Query delimiter without reasoning as a string */
#define TAGSISTANT_QUERY_DELIMITER_NO_REASONING "@@"

/** Query delimiter without reasoning as a single char */
#define TAGSISTANT_QUERY_DELIMITER_NO_REASONING_CHAR '@'

/** Alias identifier **/
#define TAGSISTANT_ALIAS_IDENTIFIER "="

/** And-set delimiter as a string */
#define TAGSISTANT_ANDSET_DELIMITER "+"

/** And-set delimiter as a single char */
#define TAGSISTANT_ANDSET_DELIMITER_CHAR '+'

/** 'Next tag should not match' indicator */
#define TAGSISTANT_NEGATE_NEXT_TAG "-"

/** 'Next tag should not match' indicator as a single char */
#define TAGSISTANT_NEGATE_NEXT_TAG_CHAR '-'

/** the string used to separate inodes from filenames inside archive/ directory */
#define TAGSISTANT_INODE_DELIMITER "___"

/** the string used to start a group of alternative tags */
#define TAGSISTANT_TAG_GROUP_BEGIN "{"

/** the string used to close a group of alternative tags */
#define TAGSISTANT_TAG_GROUP_END "}"

/** use an hash table to save previously processed querytrees */
#define TAGSISTANT_ENABLE_QUERYTREE_CACHE 1

/** cache tag IDs? */
#define TAGSISTANT_ENABLE_TAG_ID_CACHE 1

/** cache inode resolution queries? */
#define TAGSISTANT_ENABLE_AND_SET_CACHE 0

/** cache reasoner queries? */
#define TAGSISTANT_ENABLE_REASONER_CACHE 0

/** enable filehandle caching between open(), read(), write() and release() calls */
#define TAGSISTANT_ENABLE_FILE_HANDLE_CACHE 1

/** enable the autotagging plugin stack? */
#define TAGSISTANT_ENABLE_AUTOTAGGING 1

/** inline deduplication in main thread or schedule files for deduplication in a separate thread? */
#define TAGSISTANT_INLINE_DEDUPLICATION 1

/** enable filehandle caching between open(), read(), write() and release() calls */
#define TAGSISTANT_ENABLE_FILE_HANDLE_CACHING 1

/** the maximum length of the buffer used to store dynamic /stats files */
#define TAGSISTANT_STATS_BUFFER 2048

/** the maximum length of a query bookmarked as an alias */
#define TAGSISTANT_ALIAS_MAX_LENGTH 1024

/** the depth of the archive/ hierarchy (each zero adds one level, 10 is the lowest meaningful value) */
#define TAGSISTANT_ARCHIVE_DEPTH 1000

/** the default regular expression to identify the starting token of a triple tag */
#define TAGSISTANT_DEFAULT_TRIPLE_TAG_REGEX ":$"

/** the default suffix appended to files to get their tags */
#define TAGSISTANT_DEFAULT_TAGS_SUFFIX ".tags"

/** the number of tuples (rows) allowed in the rds table before the GC kicks in */
#define TAGSISTANT_GC_TUPLES 1000000

/** the number of RDS (reusable data sets) allowed in the rds table before the GC kicks in */
#define TAGSISTANT_GC_RDS 50000

/**
 * Some replacements to standard C construct to avoid common pitfalls
 * and make the source more readable
 */
#define is ==
#define isNot !=
#define eq ==

#include "config.h"

#ifndef VERSION
#define VERSION 0
#endif

#ifndef PLUGINS_DIR
#define PLUGINS_DIR "/usr/local/lib/tagsistant/"
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifdef linux
/* For pread()/pwrite() */
#ifdef _XOPEN_SOURCE
#undef _XOPEN_SOURCE
#endif
#define _XOPEN_SOURCE 500
#endif

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif

#ifndef _REENTRANT
#define _REENTRANT
#endif
#define _POSIX_PTHREAD_SEMANTICS

#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#if FUSE_USE_VERSION is 25
#	if HAVE_SYS_STATVFS_H
#		include <sys/statvfs.h>
# endif
#else
#	if HAVE_SYS_STATFS_H
#		include <sys/statfs.h>
#	endif
#endif
#include <stddef.h>
#include <netinet/in.h>
#include <utime.h>
#include <signal.h>
#include <dlfcn.h> /* for dlopen() and friends */
#include <glib.h>
#include <gio/gio.h>
#include <inttypes.h>
#include <stdint.h>

#if HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#undef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64

#include <fuse.h>
#include <extractor.h>

/** define libextractor support */
#if (EXTRACTOR_VERSION & 0x00050000) is 0x00050000
#	define TAGSISTANT_EXTRACTOR 5          // libextractor 0.5.x
#elif (EXTRACTOR_VERSION & 0x00060000) is 0x00060000
#	define TAGSISTANT_EXTRACTOR 6          // libextractor 0.6.x
#else
#	define TAGSISTANT_EXTRACTOR 0          // no support for libextractor
#endif

/**
 * each object is identified by a unique number of type tagsistant_id
 */
typedef uint32_t tagsistant_inode;

typedef uint32_t tagsistant_tag_id;

#define dyn_strcat(original, newstring) original = _dyn_strcat(original, newstring)
extern gchar *_dyn_strcat(gchar *original, const gchar *newstring);

#include "debug.h"
#include "sql.h"
#include "path_resolution.h"
#include "plugin.h"

/**
 * defines command line options for tagsistant mount tool
 */
struct tagsistant {
	gboolean	debug;			/**< debug profile */
	gboolean	no_autotagging;		/**< disable autotagging */
	gchar		*debug_flags;	/**< debug flags as a string */
	gchar 		dbg[128];		/**< debug flags */

	gboolean	foreground;		/**< run in foreground */
	gboolean	singlethread;	/**< single thread? */
	gboolean	readonly;		/**< mount filesystem readonly */
	gboolean	verbose;		/**< do verbose logging on syslog (stderr is always verbose) */
	gboolean	quiet;			/**< don't log anything, even errors */
	gboolean	show_config;	/**< show whole configuration */
	gboolean	show_help;		/**< show the help screen */
	gboolean	open_permission;/**< use relaxed permissions (777) on tags and other meta-directories */
	gboolean	enable_xattr;	/**< enable extended attributes (needed for POSIX ACL) */
	gboolean	multi_symlink;	/**< allow multiple symlinks with the same name but different targets */

	gchar		*tags_suffix;	/**< the suffix to be added to filenames to list their tags */
	gchar		*namespace_suffix; /**< the suffix that distinguishes namespaces */
	gchar		*triple_tag_regex; /**< namespace suffix detector regexp */

	gchar		*progname;		/**< tagsistant */
	gchar		*mountpoint;	/**< no clue? */
	gchar		*repository;	/**< it's where files and tags are archived, no? */
	gchar		*archive;		/**< a directory holding all the files */
	gchar		*tags;			/**< a SQLite database on file */
	gchar		*dboptions;		/**< database options for DBI */
	gchar		*link;			/**< a symlink used in getattr() for export/ */

#if TAGSISTANT_REENTRANT_DBI
	dbi_inst	dbi_instance;	/**< libDBI instance for reentrant functions */
#endif

	/** the list of available plugins */
	tagsistant_plugin_t *plugins;

	/** debug file descriptor */
	FILE *debugfd;

	/** SQL backend provides intersect keyword */
	int sql_backend_have_intersect;

	/** SQL database driver */
	int sql_database_driver;

	/** FUSE options */
	gchar **fuse_opts;

	gboolean show_version;

	/** not parsed options */
	gchar **remaining_opts;
};

/** where global values are stored */
extern struct tagsistant tagsistant;

/** the asynchronous queue used to pass files to the deduplication/autotagging thread */
extern GAsyncQueue *tagsistant_autotag_queue;

/** starts deduplication on a path */
extern void tagsistant_deduplicate(const gchar *path);

/** schedule a tagsistant_querytree for autotagging */
#if TAGSISTANT_ENABLE_AUTOTAGGING
extern void tagsistant_schedule_for_autotagging(tagsistant_querytree *qtree);
#else
#define tagsistant_schedule_for_autotagging(qtree) {}
#endif

/**
 * g_free() a symbol only if it's not NULL
 *
 * @param symbol the symbol to free
 * @return
 */
#define g_free_null(symbol) {\
	if (symbol) {\
		g_free(symbol);\
		symbol = NULL;\
	}\
}

/**
 * Check if a path contains the meta-tag ALL/
 */
#define is_all_path(path) (\
	g_regex_match_simple("/ALL/", path, G_REGEX_EXTENDED, 0) ||\
	g_regex_match_simple("/ALL$", path, G_REGEX_EXTENDED, 0))

/**
 * Fuse operations logging macros.
 * Enabled or disabled by TAGSISTANT_VERBOSE_LOGGING.
 */
#define TAGSISTANT_VERBOSE_LOGGING TRUE
#if TAGSISTANT_VERBOSE_LOGGING

#	define TAGSISTANT_START(line, ...) dbg('f', LOG_INFO, line, ##__VA_ARGS__);
#	define TAGSISTANT_STOP_OK(line, ...) dbg('f', LOG_INFO, line, ##__VA_ARGS__);
#	define TAGSISTANT_STOP_ERROR(line,...) dbg('f', LOG_ERR, line, ##__VA_ARGS__);

#else

#	define TAGSISTANT_START(line, ...) {}
#	define TAGSISTANT_STOP_OK(line, ...) {}
#	define TAGSISTANT_STOP_ERROR(line,...) {}

#endif // TAGSISTANT_VERBOSE_LOGGING

// some init functions
extern void tagsistant_utils_init();
extern void tagsistant_init_syslog();
extern void tagsistant_plugin_loader();
extern void tagsistant_plugin_unloader();
extern void tagsistant_deduplication_init();
extern void tagsistant_rds_init();

// call the plugin stack
extern int tagsistant_process(gchar *path, gchar *full_archive_path);

// used by plugins to apply regex to file content
extern void tagsistant_plugin_apply_regex(const tagsistant_querytree *qtree, const char *buf, GMutex *m, GRegex *rx);

// create and tag a new object in one single operation
#define		tagsistant_create_and_tag_object(qtree, errno) tagsistant_inner_create_and_tag_object(qtree, errno, 0);
#define		tagsistant_force_create_and_tag_object(qtree, errno) tagsistant_inner_create_and_tag_object(qtree, errno, 1);
extern int	tagsistant_inner_create_and_tag_object(tagsistant_querytree *qtree, int *tagsistant_errno, int force_create);

// check if a file ends by the tags-listing suffix (default: .tags)
extern gboolean 	tagsistant_is_tags_list_file(tagsistant_querytree *qtree);
extern gchar *		tagsistant_string_tags_list_suffix(tagsistant_querytree *qtree);

extern void tagsistant_fix_archive();

/**
 * invalidate object checksum
 *
 * @param inode the object inode
 * @param dbi_conn a valid DBI connection
 */
#define tagsistant_invalidate_object_checksum(inode, dbi_conn)\
	tagsistant_query("update objects set checksum = '' where inode = %d", dbi_conn, NULL, NULL, inode)

// read and write repository.ini file
extern GKeyFile *tagsistant_ini;
extern void tagsistant_manage_repository_ini();
extern gchar *tagsistant_get_ini_entry(gchar *section, gchar *key);
extern gchar **tagsistant_get_ini_entry_list(gchar *section, gchar *key);

extern GHashTable *tagsistant_checksummers;
extern int tagsistant_querytree_find_duplicates(tagsistant_querytree *qtree, gchar *hex);

#include "fuse_operations/operations.h"

#ifndef O_NOATIME
#define O_NOATIME	01000000
#endif

#if TAGSISTANT_ENABLE_FILE_HANDLE_CACHE
#	define tagsistant_set_file_handle(fi, fh_value) fi->fh = (unsigned long) fh_value
#	define tagsistant_get_file_handle(fi, fh_variable) fh_variable = (long) fi->fh
#else
#	define tagsistant_set_file_handle(fi, fh_value) ()
#	define tagsistant_get_file_handle(fi, fh_variable) ()
#endif

extern gchar *tagsistant_get_file_tags(tagsistant_querytree *qtree);

/**
 * RDS are Reusable Data Sets. Each RDS is build from a querytree.
 * If materialized, its entries field will list all the objects
 * matching a query.
 */
typedef struct {
	tagsistant_inode inode;
	gchar *name;
} tagsistant_rds_entry;

typedef struct {
	gchar *checksum;
	gchar *path;
	int is_all_path;
#if	TAGSISTANT_RDS_NEEDS_TREE
	qtree_or_node *tree;
#endif
	GHashTable *entries;
	GRWLock rwlock;
	GMutex materializer_mutex;
} tagsistant_rds;

extern tagsistant_rds *	tagsistant_rds_new_or_lookup(tagsistant_querytree *qtree);
extern tagsistant_rds *	tagsistant_rds_new(tagsistant_querytree *qtree);
extern void				tagsistant_delete_rds_involved(tagsistant_querytree *qtree);
extern gboolean			tagsistant_rds_materialize(tagsistant_rds *rds, tagsistant_querytree *qtree);
extern gchar *			tagsistant_get_rds_checksum(tagsistant_querytree *qtree);
extern tagsistant_rds *	tagsistant_rds_lookup(const gchar *checksum);
extern gboolean			tagsistant_rds_contains_object(gpointer key, gpointer value, gpointer user_data);

extern gboolean			tagsistant_rds_read_lock(tagsistant_rds *rds, tagsistant_querytree *qtree);
extern void				tagsistant_rds_read_unlock(tagsistant_rds *rds);
extern gboolean			tagsistant_rds_write_lock(tagsistant_rds *rds);
extern void				tagsistant_rds_write_unlock(tagsistant_rds *rds);
