/*
   Tagsistant (tagfs) -- fuse_operations/link.c
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

/**
 * link equivalent
 *
 * @param from existing file name
 * @param to new file name
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_link(const char *from, const char *to)
{
	int tagsistant_errno = 0, res = 0;

	TAGSISTANT_START("LINK %s to %s", from, to);

	/*
	 * guess if query points to an external or internal object
	 */
	char *_from = (char *) from;
	if (!TAGSISTANT_PATH_IS_EXTERNAL(from)) {
		_from = (char * ) from + strlen(tagsistant.mountpoint);
		// dbg(LOG_INFO, "%s is internal to %s, trimmed to %s", from, tagsistant.mountpoint, _from);
	}

	tagsistant_querytree *from_qtree = tagsistant_querytree_new(_from, 0, 1, 0, 0);
	tagsistant_querytree *to_qtree = tagsistant_querytree_new(to, 0, 0, 1, 0);

	from_qtree->is_external = (from == _from) ? 1 : 0;

	// -- malformed --
	if (QTREE_IS_MALFORMED(from_qtree)) TAGSISTANT_ABORT_OPERATION(ENOENT);
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
			dbg('F', LOG_INFO, "LINK : Creating %s", to_qtree->object_path);
			res = tagsistant_force_create_and_tag_object(to_qtree, &tagsistant_errno);
			if (-1 == res) goto TAGSISTANT_EXIT_OPERATION;
		} else

		// nothing to do about tags
		{
			dbg('F', LOG_ERR, "%s is not taggable!", to_qtree->full_path); // ??? why ??? should be taggable!!
		}

		// do the real link on disk
		dbg('F', LOG_INFO, "Hard-linking %s to %s", from_qtree->full_archive_path, to_qtree->object_path);
		res = link(from_qtree->full_archive_path, to_qtree->full_archive_path);
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
		TAGSISTANT_STOP_ERROR("LINK from %s to %s (%s) (%s): %d %d: %s", from, to, to_qtree->full_archive_path, tagsistant_querytree_type(to_qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(from_qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		tagsistant_querytree_destroy(to_qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK("LINK from %s to %s (%s): OK", from, to, tagsistant_querytree_type(to_qtree));
		tagsistant_querytree_destroy(from_qtree, TAGSISTANT_COMMIT_TRANSACTION);
		tagsistant_querytree_destroy(to_qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
