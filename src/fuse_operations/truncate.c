/*
   Tagsistant (tagfs) -- fuse_operations/truncate.c
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
 * truncate equivalent
 *
 * @param path the path to be truncate()ed
 * @param size truncation offset
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_truncate(const char *path, off_t size)
{
    int res = 0, tagsistant_errno = 0;

	TAGSISTANT_START(OPS_IN "TRUNCATE on %s [size: %lu]", path, (long unsigned int) size);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (tagsistant_is_tags_list_file(qtree)) {
			tagsistant_query(
				"delete from tagging where inode = %d",
				qtree->dbi, NULL, NULL, qtree->inode);
		} else {
			res = truncate(qtree->full_archive_path, size);
			tagsistant_errno = errno;
		}
	} else

	// -- alias --
	if (QTREE_IS_ALIAS(qtree) && qtree->alias) {
		tagsistant_sql_alias_set(qtree->dbi, qtree->alias, "");
		res = 0;
		tagsistant_errno = 0;
	}


	// -- tags --
	// -- stats --
	// -- relations --
	// -- alias (the directory) --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "TRUNCATE %s at %llu (%s): %d %d: %s", qtree->full_archive_path, (unsigned long long) size, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "TRUNCATE %s, %llu (%s): OK", path, (unsigned long long) size, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
