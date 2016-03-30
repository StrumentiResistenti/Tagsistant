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
 * close() equivalent [first part, second is tagsistant_release()]
 *
 * @param path the path to be open()ed
 * @param fi struct fuse_file_info holding open() flags
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_flush(const char *path, struct fuse_file_info *fi)
{
	(void) fi;

	int res = 0, tagsistant_errno = 0, do_deduplicate = 0;
	const gchar *deduplicate = NULL;

	TAGSISTANT_START("FLUSH on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	if (qtree->full_archive_path) {
		tagsistant_query(
			"select 1 from objects where objectname = '%s' and checksum = ''",
			qtree->dbi,
			tagsistant_return_integer,
			&do_deduplicate,
			qtree->object_path);

		if (do_deduplicate) {
			dbg('2', LOG_INFO, "Deduplicating %s", path);
			deduplicate = path; // was g_strdup(path);
		} else {
			dbg('2', LOG_INFO, "Skipping deduplication for %s", path);
		}
	}

	if (fi->fh) {
		dbg('F', LOG_INFO, "Uncaching %" PRIu64 " = open(%s)", fi->fh, path);
		close(fi->fh);
		fi->fh = 0;
	}

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR("FLUSH on %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		if (do_deduplicate) tagsistant_deduplicate(deduplicate);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("FLUSH on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		if (do_deduplicate) tagsistant_deduplicate(deduplicate);
		return (0);
	}
}
