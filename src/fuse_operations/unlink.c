/*
   Tagsistant (tagfs) -- fuse_operations/unlink.c
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
 * unlink equivalent
 *
 * @param path the path to be unlinked (deleted)
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_unlink(const char *path)
{
    int res = 0, tagsistant_errno = 0;
    gboolean dispose = TRUE;
	gchar *unlink_path = NULL;

	TAGSISTANT_START(OPS_IN "UNLINK on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 1, 1, 0);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- objects on disk --
	if (QTREE_IS_STORE(qtree)) {
		tagsistant_querytree_check_tagging_consistency(qtree);

		if (QTREE_IS_TAGGABLE(qtree)) {
			if (is_all_path(qtree->full_path)) {

				tagsistant_query(
					"delete from objects where inode = %d",
					qtree->dbi, NULL, NULL, qtree->inode);

				tagsistant_query(
					"delete from tagging where inode = %d",
					qtree->dbi, NULL, NULL, qtree->inode);

			} else {

				/*
				 * if object is pointed by a tags/ query, then untag it
				 * from the tags included in the query path...
				 */
				tagsistant_querytree_traverse(qtree, tagsistant_sql_untag_object, qtree->inode);

				/*
				 * ...then check if it's tagged elsewhere...
				 * ...if still tagged, then avoid real unlink(): the object must survive!
				 * ...otherwise we can delete it from the objects table
				 */
				dispose = tagsistant_dispose_object_if_untagged(qtree);
			}

#if TAGSISTANT_ENABLE_AND_SET_CACHE
			/*
			 * invalidate the and_set cache
			 */
			tagsistant_invalidate_and_set_cache_entries(qtree);
#endif


			/*
			 * clean the RDS library
			 */
			tagsistant_delete_rds_involved(qtree);
		}

		// unlink the object on disk
		if (dispose) {
			unlink_path = qtree->full_archive_path;
			res = unlink(unlink_path);
			tagsistant_errno = errno;
		}
	} else

	// -- alias --
	if (QTREE_IS_ALIAS(qtree)) {
		tagsistant_sql_alias_delete(qtree->dbi, qtree->alias);
	}

	// -- tags --
	// -- stats --
	// -- relations --
	// -- archive --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "UNLINK on %s (%s) (%s): %d %d: %s", path, unlink_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "UNLINK on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
