/*
   Tagsistant (tagfs) -- fuse_operations/read.c
   Copyright (C) 2006-2014 Tx0 <tx0@strumentiresistenti.org>

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

#include "../tagsistant.h"

void tagsistant_read_stats_configuration(gchar stats_buffer[TAGSISTANT_STATS_BUFFER]);

/**
 * read() equivalent
 *
 * @param path the path of the file to be read
 * @param buf buffer holding read() result
 * @param size how many bytes should/can be read
 * @param offset starting of the read
 * @param fi struct fuse_file_info used for open() flags
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0, fh = 0;
    gchar stats_buffer[TAGSISTANT_STATS_BUFFER];

	TAGSISTANT_START(OPS_IN "READ on %s [size: %lu offset: %lu]", path, (long unsigned int) size, (long unsigned int) offset);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- error message --
	if (qtree->error_message && g_regex_match_simple("@/error$", path, G_REGEX_EXTENDED, 0)) {
		memcpy(buf, qtree->error_message, strlen(qtree->error_message));
		res = strlen(qtree->error_message);
	}

	// -- object on disk --
	else if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (tagsistant_is_tags_list_file(qtree)) {
			/* get the tag list */
			gchar *tags_list = tagsistant_get_file_tags(qtree);
			if (!tags_list) TAGSISTANT_ABORT_OPERATION(EFAULT);

			/* copy the tag list into the FUSE buffer */
			memcpy(buf, tags_list, size);
			g_free(tags_list);

			/* set the return value and exit */
			res = strlen(buf);
			goto TAGSISTANT_EXIT_OPERATION;
		}

		if (!qtree->full_archive_path) {
			dbg('F', LOG_ERR, "Null qtree->full_archive_path");
			TAGSISTANT_ABORT_OPERATION(EFAULT);
		}

#if TAGSISTANT_ENABLE_FILE_HANDLE_CACHING
		if (fi->fh) {
			tagsistant_get_file_handle(fi, fh);
			res = pread(fh, buf, size, offset);
			tagsistant_errno = errno;
//			fprintf(stderr, "Trying a read on FD %lu\n", fi->fh);
		}

		if ((res is -1) || (fh is 0)) {
			if (fh) close(fh);
			fh = open(qtree->full_archive_path, fi->flags|O_RDONLY);
//			fprintf(stderr, "Re-trying a read on FD %lu\n", fi->fh);

			if (fh)	res = pread(fh, buf, size, offset);
			else res = -1;
			tagsistant_errno = errno;
		}

		tagsistant_set_file_handle(fi, fh);
#else
		fh = open(qtree->full_archive_path, fi->flags|O_RDONLY);
		if (fh) {
			res = pread(fh, buf, size, offset);
			tagsistant_errno = errno;
			close(fh);
		} else {
			TAGSISTANT_ABORT_OPERATION(errno);
		}
#endif
	}

	// -- alias --
	else if (QTREE_IS_ALIAS(qtree)) {
		gchar *value = NULL;
		tagsistant_query(
			"select query from aliases where alias = '%s'",
			qtree->dbi,
			tagsistant_return_string,
			&value,
			qtree->alias);

		if (value) {
			res = strlen(value);
			memcpy(buf, value, res);
		}
	}

	// -- stats --
	else if (QTREE_IS_STATS(qtree)) {
		memset(stats_buffer, 0, TAGSISTANT_STATS_BUFFER);

		// -- connections --
		if (g_regex_match_simple("/connections$", path, 0, 0)) {
			sprintf(stats_buffer, "# of MySQL open connections: %d\n", tagsistant_active_connections);
		}

#if TAGSISTANT_ENABLE_QUERYTREE_CACHE
		// -- cached_queries --
		else if (g_regex_match_simple("/cached_queries$", path, 0, 0)) {
			int entries = tagsistant_querytree_cache_total();
			sprintf(stats_buffer, "# of cached queries: %d\n", entries);
		}
#endif /* TAGSISTANT_ENABLE_QUERYTREE_CACHE */

		// -- configuration --
		else if (g_regex_match_simple("/configuration$", path, 0, 0)) {
			tagsistant_read_stats_configuration(stats_buffer);
		}

		// -- objects --
		else if (g_regex_match_simple("/objects$", path, 0, 0)) {
			int entries = 0;
			tagsistant_query("select count(1) from objects", qtree->dbi, tagsistant_return_integer, &entries);
			sprintf(stats_buffer, "# of objects: %d\n", entries);
		}

		// -- tags --
		else if (g_regex_match_simple("/tags$", path, 0, 0)) {
			int entries = 2;
			tagsistant_query("select count(1) from tags", qtree->dbi, tagsistant_return_integer, &entries);
			sprintf(stats_buffer, "# of tags: %d\n", entries);
		}

		// -- relations --
		else if (g_regex_match_simple("/relations$", path, 0, 0)) {
			int entries = 0;
			tagsistant_query("select count(1) from relations", qtree->dbi, tagsistant_return_integer, &entries);
			sprintf(stats_buffer, "# of relations: %d\n", entries);
		}

		size_t stats_size = strlen(stats_buffer);
		if ((size_t) offset <= stats_size) {
			gchar *start = stats_buffer + offset;
			size_t available = stats_size - offset;
			size_t real_size = (size > available) ? available : size;

			memcpy(buf, start, real_size);
			res = (int) real_size;
		}
	}

	// -- tags --
	// -- relations --
	else TAGSISTANT_ABORT_OPERATION(EINVAL);

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "READ %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "READ %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (res);
	}
}

void tagsistant_read_stats_configuration(gchar stats_buffer[TAGSISTANT_STATS_BUFFER])
{
	snprintf(stats_buffer, TAGSISTANT_STATS_BUFFER,
		"\n"
		" --> Command line options:\n\n"
		"         mountpoint: %s\n"
		"    repository path: %s\n"
		"   database options: %s\n"
		"        tags suffix: %s (append it to object names to list their tags)\n"
		"  run in foreground: %d\n"
		"    single threaded: %d\n"
		"    mount read-only: %d\n"
		"              debug: %s\n"
		"                     [%c] boot\n"
		"                     [%c] cache\n"
		"                     [%c] file tree (readdir)\n"
		"                     [%c] FUSE operations (open, read, write, symlink, ...)\n"
		"                     [%c] low level\n"
		"                     [%c] plugin\n"
		"                     [%c] query parsing\n"
		"                     [%c] reasoning\n"
		"                     [%c] SQL queries\n"
		"                     [%c] deduplication\n"
		"\n"
		" --> Compile flags:\n\n"
		"    TAGSISTANT_ENABLE_QUERYTREE_CACHE: %d\n"
		"       TAGSISTANT_ENABLE_TAG_ID_CACHE: %d\n"
		"      TAGSISTANT_ENABLE_AND_SET_CACHE: %d\n"
		"     TAGSISTANT_ENABLE_REASONER_CACHE: %d\n"
		"  TAGSISTANT_ENABLE_FILE_HANDLE_CACHE: %d\n"
		"        TAGSISTANT_ENABLE_AUTOTAGGING: %d\n"
		"           TAGSISTANT_QUERY_DELIMITER: %c (to avoid reasoning use: %s)\n"
		"          TAGSISTANT_ANDSET_DELIMITER: %c\n"
		"           TAGSISTANT_INODE_DELIMITER: '%s'\n"
		"           TAGSISTANT_TAG_GROUP_BEGIN: %s\n"
		"             TAGSISTANT_TAG_GROUP_END: %s\n"
		"  TAGSISTANT_DEFAULT_TRIPLE_TAG_REGEX: %s\n"
		"       TAGSISTANT_DEFAULT_TAGS_SUFFIX: %s\n"
		"                 TAGSISTANT_GC_TUPLES: %d\n"
		"                    TAGSISTANT_GC_RDS: %d\n"
		"\n",
		tagsistant.mountpoint,
		tagsistant.repository,
		tagsistant.dboptions,
		tagsistant.tags_suffix,
		tagsistant.foreground,
		tagsistant.singlethread,
		tagsistant.readonly,
		tagsistant.debug_flags ? tagsistant.debug_flags : "-",
		tagsistant.dbg['b'] ? 'x' : ' ',
		tagsistant.dbg['c'] ? 'x' : ' ',
		tagsistant.dbg['f'] ? 'x' : ' ',
		tagsistant.dbg['F'] ? 'x' : ' ',
		tagsistant.dbg['l'] ? 'x' : ' ',
		tagsistant.dbg['p'] ? 'x' : ' ',
		tagsistant.dbg['q'] ? 'x' : ' ',
		tagsistant.dbg['r'] ? 'x' : ' ',
		tagsistant.dbg['s'] ? 'x' : ' ',
		tagsistant.dbg['2'] ? 'x' : ' ',
		TAGSISTANT_ENABLE_QUERYTREE_CACHE,
		TAGSISTANT_ENABLE_TAG_ID_CACHE,
		TAGSISTANT_ENABLE_AND_SET_CACHE,
		TAGSISTANT_ENABLE_REASONER_CACHE,
		TAGSISTANT_ENABLE_FILE_HANDLE_CACHE,
		TAGSISTANT_ENABLE_AUTOTAGGING,
		TAGSISTANT_QUERY_DELIMITER_CHAR, TAGSISTANT_QUERY_DELIMITER_NO_REASONING,
		TAGSISTANT_ANDSET_DELIMITER_CHAR,
		TAGSISTANT_INODE_DELIMITER,
		TAGSISTANT_TAG_GROUP_BEGIN,
		TAGSISTANT_TAG_GROUP_END,
		TAGSISTANT_DEFAULT_TRIPLE_TAG_REGEX,
		TAGSISTANT_DEFAULT_TAGS_SUFFIX,
		TAGSISTANT_GC_TUPLES,
		TAGSISTANT_GC_RDS
	);
}
