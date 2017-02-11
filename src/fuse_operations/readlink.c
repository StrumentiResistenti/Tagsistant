/*
   Tagsistant (tagfs) -- fuse_operations/readlink.c
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
 * readlink equivalent
 *
 * @param path the path of the symlink to be read
 * @param buf the path the symlink is pointing to
 * @param size length of pointed path
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_readlink(const char *path, char *buf, size_t size)
{
    int res = 0, tagsistant_errno = 0;
	gchar *readlink_path = NULL;

	TAGSISTANT_START(OPS_IN "READLINK on %s", path);

	// build querytree
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree)) {
		TAGSISTANT_ABORT_OPERATION(ENOENT);
	}

	// -- store (complete) --
	// -- archive --
	if ((QTREE_IS_STORE(qtree) && QTREE_IS_COMPLETE(qtree)) || QTREE_IS_ARCHIVE(qtree)) {
		readlink_path = qtree->object_path;
		readlink_path = qtree->full_archive_path;
	}

	// -- export --
	else if (QTREE_IS_EXPORT(qtree)) {
		if (!qtree->inode) { TAGSISTANT_ABORT_OPERATION(EIO); }

		/*
		 * just to be sure, rebuild the querytree internal paths
		 */
		tagsistant_querytree_rebuild_paths(qtree);

		/*
		 * go back once for the tag and again for the export/ dir,
		 * then enter archive, the whole stack of reversed inode and
		 * finally put the filename here
		 */
		gchar *tmp = g_strdup_printf("../../archive%s", qtree->archive_path);

		/*
		 * get the length of the link contents and guess if the FUSE
		 * buffer is big enough to hold it. Otherwise return just part
		 * of the content
		 */
		size_t tmp_len = strlen(tmp);
		res = (size - 1 < tmp_len) ? size - 1 : tmp_len;

		/*
		 * fill the buffer and terminate it
		 */
		memcpy(buf, tmp, res);
		buf[res] = '\0';

		/*
		 * clean and skip the rest
		 */
		g_free(tmp);
		goto TAGSISTANT_EXIT_OPERATION;
	}

	// -- alias --
	// -- stats --
	// -- tags --
	// -- store (not complete) --
	// -- relations --
	else TAGSISTANT_ABORT_OPERATION(EINVAL);

	// do real readlink()
	res = readlink(readlink_path, buf, size);
	tagsistant_errno = errno;

	// fix bug #12475
	if (res > 0) buf[res] = '\0';

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "READLINK on %s (%s) (%s): %d %d: %s", path, readlink_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "REALINK on %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (0);
	}
}
