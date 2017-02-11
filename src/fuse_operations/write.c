/*
   Tagsistant (tagfs) -- fuse_operations/write.c
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
 * write() equivalent
 *
 * @param path the path of the file to be written
 * @param buf buffer holding write() data
 * @param size how many bytes should be written (size of *buf)
 * @param offset starting of the write
 * @param fi struct fuse_file_info used for open() flags
 * @return(0 on success, -errno otherwise)
 */
int tagsistant_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int res = 0, tagsistant_errno = 0, fh = 0;

	TAGSISTANT_START(OPS_IN "WRITE on %s [size: %lu offset: %lu]", path, (unsigned long) size, (long unsigned int) offset);

	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	// -- malformed --
	if (QTREE_IS_MALFORMED(qtree))
		TAGSISTANT_ABORT_OPERATION(ENOENT);

	// -- alias --
	if (QTREE_IS_ALIAS(qtree) && qtree->alias) {
		res = size;

		gchar *_buf = g_strndup(buf, size);

		// end the string at the first carriage return or line feed character
		gchar *path_ptr = rindex(_buf, '\n');
		if (path_ptr) *path_ptr = '/';

		path_ptr = rindex(_buf, '\r');
		if (path_ptr) *path_ptr = '/';

		// remove double slashes
		GRegex *rx = g_regex_new("//", 0, 0, NULL);
		gchar *_buf2 = g_regex_replace(rx, _buf, -1, 0, "/", 0, NULL);
		g_regex_unref(rx);
		g_free(_buf);
		_buf = _buf2;

		// get the size of the buffer
		size_t max_size = MIN(size, TAGSISTANT_ALIAS_MAX_LENGTH - 1);
		size_t real_size = MIN(max_size, strlen(_buf));

		// copy the buffer to a temporary variable
		gchar *value = g_strndup(_buf, real_size);

		// save the buffer on disk
		tagsistant_sql_alias_set(qtree->dbi, qtree->alias, value);

		g_free(value);
		g_free(_buf);

	} else

	// -- object on disk --
	if (QTREE_POINTS_TO_OBJECT(qtree)) {
		if (tagsistant_is_tags_list_file(qtree)) {
			tagsistant_delete_rds_involved(qtree);

			/*
			 * build a tagsistant_querytree without the .tags suffix
			 * just to guess object inode
			 */
			gchar *object_path = tagsistant_string_tags_list_suffix(qtree);
			tagsistant_querytree *object_qtree = tagsistant_querytree_new(object_path, 0, 0, 0, 1);
			tagsistant_inode inode = object_qtree->inode;
			tagsistant_delete_rds_involved(object_qtree);
			tagsistant_querytree_destroy(object_qtree, 0);
			g_free(object_path);

			/*
			 * delete current tagging
			 */
			tagsistant_query(
				"delete from tagging where inode = %d",
				qtree->dbi, NULL, NULL, inode);

			/*
			 * split the buffer into tokens
			 */
			gchar *buf_term = g_strndup(buf, size);
			gchar **tags = g_strsplit(buf_term, "\n", -1);
			gchar **token = tags;
			g_free(buf_term);

			/*
			 * iterate on tokens
			 */
			while (*token) {
				tagsistant_sql_smart_tag_object(qtree->dbi, *token, inode);
				token++;
			}

			/*
			 * free the tokens and return
			 */
			g_strfreev(tags);
			res = size;
			goto TAGSISTANT_EXIT_OPERATION;
		}

		if (!qtree->full_archive_path) {
			dbg('F', LOG_ERR, "Null qtree->full_archive_path");
			TAGSISTANT_ABORT_OPERATION(EFAULT);
		}

#if TAGSISTANT_ENABLE_FILE_HANDLE_CACHING
		if (fi->fh) {
			tagsistant_get_file_handle(fi, fh);
			res = pwrite(fh, buf, size, offset);
			tagsistant_errno = errno;
		}

		if ((res is -1) || (fh is 0)) { // TODO or was it "fs is -1"?
			if (fh) close(fh);
			fh = open(qtree->full_archive_path, fi->flags|O_WRONLY);
			if (fh)	res = pwrite(fh, buf, size, offset);
			else res = -1;
			tagsistant_errno = errno;
		}

		tagsistant_set_file_handle(fi, fh);
#else
		fh = open(qtree->full_archive_path, fi->flags|O_WRONLY);
		if (fh) {
			res = pwrite(fh, buf, size, offset);
			tagsistant_errno = errno;
			close(fh);
		} else {
			TAGSISTANT_ABORT_OPERATION(errno);
		}
#endif
	}

	// -- tags --
	// -- stats --
	// -- relations --
	else TAGSISTANT_ABORT_OPERATION(EROFS);

	// dbg('F', LOG_ERR, "Yeah!");

TAGSISTANT_EXIT_OPERATION:
	if ( res is -1 ) {
		TAGSISTANT_STOP_ERROR(OPS_OUT "WRITE %s (%s) (%s): %d %d: %s", path, qtree->full_archive_path, tagsistant_querytree_type(qtree), res, tagsistant_errno, strerror(tagsistant_errno));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_ROLLBACK_TRANSACTION);
		return (-tagsistant_errno);
	} else {
		TAGSISTANT_STOP_OK(OPS_OUT "WRITE %s (%s): OK", path, tagsistant_querytree_type(qtree));
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return (res);
	}
}
