/*
   Tagsistant (tagfs) -- fuse_operations/symlink.c
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

int tagsistant_create_symlink(tagsistant_querytree *to_qtree, const gchar *from, int *tagsistant_errno)
{
	dbg('F', LOG_INFO, "SYMLINK : Creating %s", to_qtree->object_path);

	int res = tagsistant_force_create_and_tag_object(to_qtree, tagsistant_errno);

	if (res isNot -1) {
		// save the target path for future checks
		tagsistant_query(
			"update objects set symlink = '%s' where inode = %d",
			to_qtree->dbi,
			NULL, NULL,
			from, to_qtree->inode);
	}

	return (res);
}

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
		if (strlen(to_qtree->object_path) is 0) {
			dbg('F', LOG_INFO, "Getting object path from %s", from);
			tagsistant_querytree_set_object_path(to_qtree, g_path_get_basename(from));
		}

		tagsistant_querytree_check_tagging_consistency(to_qtree);

		// if qtree is taggable, do it
		if (QTREE_IS_TAGGABLE(to_qtree)) {
			if (tagsistant.multi_symlink) {

				tagsistant_rds *rds = tagsistant_rds_new_or_lookup(to_qtree);
				if (rds) {
					/*
					 * fetch the inodes that map to the "to" path
					 */
					tagsistant_rds_read_lock(rds, to_qtree);
					GList *inodes = g_hash_table_lookup(rds->entries, to_qtree->object_path);
					tagsistant_rds_read_unlock(rds);

					if (inodes) {
						/*
						 * form a string of comma separated inodes
						 */
						GString *i = g_string_new("");
						while (inodes) {
							g_string_append_printf(i, "%d", GPOINTER_TO_UINT(inodes->data));
							if (inodes->next) g_string_append_printf(i, ", ");
						}

						/*
						 * check if one of the inodes it's a symlink
						 */
						tagsistant_inode check_inode = 0;
						tagsistant_query(
							"select inode from objects "
								"where objectname = \"%s\" "
								"and symlink = \"%s\" "
								"and inode in (%s)",
							to_qtree->dbi,
							tagsistant_return_integer, &check_inode,
							to_qtree->object_path, from, i->str);

						g_string_free(i, TRUE);

						if (check_inode) {

							/*
							 * tag the available symlink with the new tag set
							 */
							dbg('F', LOG_INFO, "SYMLINK : Deduplicating on inode %d", check_inode);
							tagsistant_querytree_traverse(to_qtree, tagsistant_sql_tag_object, check_inode);
							g_assert(res is 0);
							goto TAGSISTANT_EXIT_OPERATION;

						} else {

							/*
							 * just create the object
							 */
							res = tagsistant_create_symlink(to_qtree, from, &tagsistant_errno);
							if (res is -1) goto TAGSISTANT_EXIT_OPERATION;

						}
					} else {

						/*
						 * no inode returned, just create the object
						 */
						res = tagsistant_create_symlink(to_qtree, from, &tagsistant_errno);
						if (res is -1) goto TAGSISTANT_EXIT_OPERATION;

					}
				} else {

					/*
					 * 2. just create the object (we should never reach this code, because
					 *    it means that the RDS has not been created!)
					 */
					dbg('F', LOG_ERR, "Unable to get an RDS when symlink(%s, %s)", from, to_qtree->full_path);
					res = tagsistant_create_symlink(to_qtree, from, &tagsistant_errno);
					if (res is -1) goto TAGSISTANT_EXIT_OPERATION;

				}

			} else {

				tagsistant_inode check_inode = 0;

				/*
				 * check if a symlink pointing to "from" path is already in the DB
				 */
				tagsistant_query(
					"select inode from objects where symlink = '%s'",
					to_qtree->dbi,
					tagsistant_return_integer,
					&check_inode,
					from);

				if (check_inode) {

					/*
					 * tag the available symlink with the new tag set
					 */
					dbg('F', LOG_INFO, "SYMLINK : Deduplicating on inode %d", check_inode);
					tagsistant_querytree_traverse(to_qtree, tagsistant_sql_tag_object, check_inode);
					tagsistant_delete_rds_involved(to_qtree);
					goto TAGSISTANT_EXIT_OPERATION; // skip the real symlink

				} else {

					/*
					 * create a new symlink
					 */
					res = tagsistant_create_symlink(to_qtree, from, &tagsistant_errno);
					if (res is -1) goto TAGSISTANT_EXIT_OPERATION;
				}
			}

		} else {
			/*
			 * not taggable??? why??? it should be taggable!!
			 */
			dbg('F', LOG_ERR, "%s is not taggable!", to_qtree->full_path);
			TAGSISTANT_ABORT_OPERATION(EINVAL);
		}

		/*
		 * do the real symlink on disk
		 */
		dbg('F', LOG_INFO, "Symlinking %s to %s", from, to_qtree->object_path);
		res = symlink(from, to_qtree->full_archive_path);
		tagsistant_errno = errno;

		/*
		 * clean the RDS library
		 */
		tagsistant_delete_rds_involved(to_qtree);

		/*
		 * schedule the symlink for autotagging
		 */
		tagsistant_schedule_for_autotagging(to_qtree);
	}

	// -- store (not complete) --
	// -- tags --
	// -- stats --
	// -- relations --
	// -- alias --
	else TAGSISTANT_ABORT_OPERATION(EINVAL);

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR("SYMLINK from %s to %s (%s) (%s): %d %d: %s", from, to, to_qtree->full_archive_path, tagsistant_querytree_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(to_qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("SYMLINK from %s to %s (%s): OK", from, to, tagsistant_querytree_type(to_qtree));
		tagsistant_querytree_destroy(to_qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
