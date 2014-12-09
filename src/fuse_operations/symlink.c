/*
   Tagsistant (tagfs) -- fuse_operations/symlink.c
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

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

int tagsistant_symlink(const char *from, const char *to)
{
	int tagsistant_errno = 0, res = 0;

	TAGSISTANT_START("SYMLINK %s to %s", from, to);

	tagsistant_querytree *to_qtree = tagsistant_querytree_new(to, 0, 1, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(to_qtree)) TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(to_qtree) || (QTREE_IS_STORE(to_qtree) && QTREE_IS_COMPLETE(to_qtree))) {

		// if object_path is null, borrow it from original path
		if (strlen(to_qtree->object_path) == 0) {
			dbg('F', LOG_INFO, "Getting object path from %s", from);
			tagsistant_querytree_set_object_path(to_qtree, g_path_get_basename(from));
		}

		tagsistant_querytree_check_tagging_consistency(to_qtree);

		// if qtree is taggable, do it
		if (QTREE_IS_TAGGABLE(to_qtree)) {
			// 1. check if a symlink pointing to "from" path is already in the DB
			tagsistant_inode check_inode = 0;
			tagsistant_query(
				"select inode from objects where symlink = '%s'",
				to_qtree->dbi,
				tagsistant_return_integer,
				&check_inode,
				from);

			if (check_inode) {

				// 2.1. tag the available symlink with the new tag set
				dbg('F', LOG_INFO, "SYMLINK : Deduplicating on inode %d", check_inode);
				tagsistant_querytree_traverse(to_qtree, tagsistant_sql_tag_object, check_inode);
				goto TAGSISTANT_EXIT_OPERATION;

			} else {

				// 2.1. create a new symlink
				dbg('F', LOG_INFO, "SYMLINK : Creating %s", to_qtree->object_path);
				res = tagsistant_force_create_and_tag_object(to_qtree, &tagsistant_errno);
				if (-1 == res) goto TAGSISTANT_EXIT_OPERATION;

				// 2.2. save the target path for future checks
				tagsistant_query(
					"update objects set symlink = '%s' where inode = %d",
					to_qtree->dbi,
					NULL, NULL,
					from, to_qtree->inode);
			}

			// clean the RDS library
			tagsistant_delete_rds_involved(to_qtree);
		} else

		// nothing to do about tags
		{
			dbg('F', LOG_ERR, "%s is not taggable!", to_qtree->full_path); // ??? why ??? should be taggable!!
		}

		// do the real symlink on disk
		dbg('F', LOG_INFO, "Symlinking %s to %s", from, to_qtree->object_path);
		res = symlink(from, to_qtree->full_archive_path);
		tagsistant_errno = errno;
	}

	// -- store (not complete) --
	// -- tags --
	// -- stats --
	// -- relations --
	// -- alias --
	else TAGSISTANT_ABORT_OPERATION(EINVAL);

TAGSISTANT_EXIT_OPERATION:
	if ( res == -1 ) {
		TAGSISTANT_STOP_ERROR("SYMLINK from %s to %s (%s) (%s): %d %d: %s", from, to, to_qtree->full_archive_path, tagsistant_querytree_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(to_qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("SYMLINK from %s to %s (%s): OK", from, to, tagsistant_querytree_type(to_qtree));
		tagsistant_querytree_destroy(to_qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
