/*
   Tagsistant (tagfs) -- fuse_operations/open.c
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

/**
 * open() equivalent
 *
 * @param path the path to be open()ed
 * @param fi struct fuse_file_info holding open() flags
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_open(const char *path, struct fuse_file_info *fi)
{
    int res = -1, tagsistant_errno = ENOENT;

	TAGSISTANT_START("OPEN on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- error message --
	if (qtree->error_message && g_regex_match_simple("@/error$", path, G_REGEX_EXTENDED, 0)) {
		res = 1;
		tagsistant_errno = 0;
		goto TAGSISTANT_EXIT_OPERATION;
	}

	// -- object --
	else if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (tagsistant_is_tags_list_file(qtree)) {
			res = open(tagsistant.tags, fi->flags|O_RDONLY);
			tagsistant_errno = errno;
			goto TAGSISTANT_EXIT_OPERATION;
		}

		if (!qtree->full_archive_path) {
			dbg('F', LOG_ERR, "Null qtree->full_archive_path");
			TAGSISTANT_ABORT_OPERATION(EFAULT);
		}

		res = open(qtree->full_archive_path, fi->flags /*|O_RDONLY */);
		tagsistant_errno = errno;

		if (res isNot -1) {
#if TAGSISTANT_ENABLE_FILE_HANDLE_CACHING
			tagsistant_set_file_handle(fi, res);
			dbg('F', LOG_INFO, "Caching %" PRIu64 " = open(%s)", fi->fh, path);
//			fprintf(stderr, "Opened FD %lu\n", fi->fh);

#else
			close(res);
#endif

			tagsistant_querytree_check_tagging_consistency(qtree);

			if (QTREE_IS_TAGGABLE(qtree)) {
				if ((fi->flags & O_WRONLY) || (fi->flags & O_RDWR)) {
					// invalidate the checksum
					dbg('2', LOG_INFO, "Invalidating checksum on %s", path);
					tagsistant_invalidate_object_checksum(qtree->inode, qtree->dbi);
				} else {
					fi->keep_cache = 1;
				}
			}
		} else {
			tagsistant_set_file_handle(fi, 0);
		}
	}

	// -- stats --
	else if (QTREE_IS_STATS(qtree)) {
		res = open(tagsistant.tags, fi->flags|O_RDONLY);
		tagsistant_set_file_handle(fi, res);
		tagsistant_errno = errno;
		fi->keep_cache = 0;
	}

	// -- alias --
	else if (QTREE_IS_ALIAS(qtree) && qtree->alias) {
		if (tagsistant_sql_alias_exists(qtree->dbi, qtree->alias)) {
			res = 0;
			tagsistant_errno = 0;
		} else {
			TAGSISTANT_ABORT_OPERATION(ENOENT);
		}
	}

	// -- tags --
	// -- relations --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR("OPEN on %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("OPEN on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
