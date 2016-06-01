/*
   Tagsistant (tagfs) -- deduplication.c
   Copyright (C) 2006-2014 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.

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

#include "tagsistant.h"
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#define TAGSISTANT_AUTOTAGGING_SEPARATOR "<><><>"

/****************************************************************************/
/***                                                                      ***/
/***   Checksumming and deduplication support                             ***/
/***                                                                      ***/
/****************************************************************************/

/** deduplication queue */
#if ! TAGSISTANT_INLINE_DEDUPLICATION
GAsyncQueue *tagsistant_deduplication_queue;
#endif

/** autotagging queue */
GAsyncQueue *tagsistant_autotagging_queue;

#define TAGSISTANT_DO_AUTOTAGGING 1
#define TAGSISTANT_DONT_DO_AUTOTAGGING 0

/**
 * deduplication function called by tagsistant_calculate_object_checksum
 *
 * @param inode the object inode
 * @param hex the checksum string
 * @param dbi DBI connection handle
 * @return true if autotagging is requested, false otherwise
 */
int tagsistant_querytree_find_duplicates(tagsistant_querytree *qtree, gchar *hex)
{
	tagsistant_inode main_inode = 0;

	/*
	 * check if this file is a directory
	 */
	struct stat st;
	int result = lstat(qtree->full_archive_path, &st);
	if (result is 0 && S_ISDIR(st.st_mode)) {
		dbg('2', LOG_INFO, "%s is a directory, skipping deduplication and autotagging", qtree->full_archive_path);
		return (TAGSISTANT_DONT_DO_AUTOTAGGING);
	}

	/*
	 * get the first inode matching the checksum
	 */
	tagsistant_query(
		"select inode from objects where checksum = '%s' order by inode limit 1",
		qtree->dbi,	tagsistant_return_integer, &main_inode,	hex);

	/*
	 * if main_inode is zero, something gone wrong, we must
	 * return here, but auto-tagging can be performed
	 */
	if (!main_inode) {
		dbg('2', LOG_ERR, "Inode 0 returned for checksum %s", hex);
		return (TAGSISTANT_DO_AUTOTAGGING);
	}

	/*
	 * if this is the only copy of the file, we can
	 * return and auto-tagging can be performed
	 */
	if (qtree->inode is main_inode) return (TAGSISTANT_DO_AUTOTAGGING);

	dbg('2', LOG_INFO, "Deduplicating %s: %d -> %d", qtree->full_archive_path, qtree->inode, main_inode);

	/*
	 * first move all the tags of qtree->inode to main_inode
	 */
	if (tagsistant.sql_database_driver is TAGSISTANT_DBI_SQLITE_BACKEND) {
		tagsistant_query(
			"update or ignore tagging set inode = %d where inode = %d",
			qtree->dbi,	NULL, NULL,	main_inode,	qtree->inode);
	} else if (tagsistant.sql_database_driver is TAGSISTANT_DBI_MYSQL_BACKEND) {
		tagsistant_query(
			"update ignore tagging set inode = %d where inode = %d",
			qtree->dbi,	NULL, NULL,	main_inode,	qtree->inode);
	}

	/*
	 * then delete records left because of duplicates in key(inode, tag_id) in the tagging table
	 */
	tagsistant_query(
		"delete from tagging where inode = %d",
		qtree->dbi,	NULL, NULL,	qtree->inode);

	/*
	 * unlink the removable inode
	 */
	tagsistant_query(
		"delete from objects where inode = %d",
		qtree->dbi, NULL, NULL,	qtree->inode);

	/*
	 * and finally delete it from the archive directory
	 */
	qtree->schedule_for_unlink = 1;

	/*
	 * invalidate the RDS cache
	 */
	tagsistant_delete_rds_involved(qtree);

#if TAGSISTANT_ENABLE_AND_SET_CACHE
	/*
	 * invalidate the and_set cache
	 */
	tagsistant_invalidate_and_set_cache_entries(qtree);
#endif

	// don't do autotagging, the file has gone
	return (TAGSISTANT_DONT_DO_AUTOTAGGING);
}

/**
 * Schedule a tagsistant_querytree for autotagging
 *
 * @param qtree the tagsistant_querytree to be scheduled
 */
#if TAGSISTANT_ENABLE_AUTOTAGGING
void tagsistant_schedule_for_autotagging(tagsistant_querytree *qtree)
{
	/*
	 * check if autotagging was disabled from command line
	 */
	if (tagsistant.no_autotagging is FALSE) return;

	gchar *paths = g_strdup_printf("%s%s%s",
		qtree->full_path, TAGSISTANT_AUTOTAGGING_SEPARATOR, qtree->full_archive_path);

	dbg('p', LOG_INFO, "Running autotagging on %s", qtree->object_path);

	/*
	 * the object is eligible for autotagging,
	 * so we submit it into the autotagging queue
	 */
	g_async_queue_push(tagsistant_autotagging_queue, paths);
}
#endif

/**
 * kernel of the deduplication thread
 *
 * @param data the path to be deduplicated (must be casted back to gchar*)
 */
gpointer tagsistant_deduplication_kernel(gpointer data)
{
	gchar *path = (gchar *) data;

	// dbg('2', LOG_INFO, "Deduplication request for %s", path);

	/*
	 * create a qtree object just to extract the full_archive_path
	 */
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 0, 1, 1);

	if (qtree) {
		int fd = open(qtree->full_archive_path, O_RDONLY|O_NOATIME);
		// tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);

		if (fd isNot -1) {
			dbg('2', LOG_INFO, "Running deduplication on %s", path);

			GChecksum *checksum = g_checksum_new(G_CHECKSUM_SHA1);
			guchar buffer[65535];

			if (checksum) {
				/* feed the checksum object */
				int length = 0;
				do {
					length = read(fd, buffer, 65535);
					g_checksum_update(checksum, buffer, length);
				} while (length > 0);

				/* get the hexadecimal checksum string */
				gchar *hex = g_strdup(g_checksum_get_string(checksum));

				/* destroy the checksum object */
				g_checksum_free(checksum);

				/*
				 * save the string into the objects table
				 */
				tagsistant_query(
					"update objects set checksum = '%s' where inode = %d",
					qtree->dbi, NULL, NULL, hex, qtree->inode);

				/*
				 * look for duplicated objects
				 */
				if (tagsistant_querytree_find_duplicates(qtree, hex)) {
					/*
					 * before destroying the qtree, we build the string
					 * to schedule the object for autotagging
					 */
					tagsistant_schedule_for_autotagging(qtree);
				}

				/* free the hex checksum string */
				g_free_null(hex);
			}
		}

		close(fd);
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
	}

	return (NULL);
}

/**
 * kernel of the autotagging thread
 *
 * @param data the path to be autotagged (must be casted back to gchar*)
 */
gpointer tagsistant_autotagging_kernel(gpointer data)
{
	gchar *paths = (gchar *) data;
	if (strlen(paths) <= strlen(TAGSISTANT_AUTOTAGGING_SEPARATOR)) return(NULL);

	/*
	 * split the queued element by TAGSISTANT_AUTOTAGGING_SEPARATOR to
	 * get back the original path [0] and the full_archive_path [1]
	 */
	gchar **splitted_paths = g_strsplit(paths, TAGSISTANT_AUTOTAGGING_SEPARATOR, 2);
	gchar *path = splitted_paths[0];
	gchar *full_archive_path = splitted_paths[1];

	/*
	 * call the plugin processors
	 */
	tagsistant_process(path, full_archive_path);

	/*
	 * clean up the string vector and quit
	 * (paths will be freed by the calling function)
	 */
	g_strfreev(splitted_paths);

	return (NULL);
}

#if ! TAGSISTANT_INLINE_DEDUPLICATION
/**
 * This is the loop that calls the deduplication thread
 * when TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS is 0
 */
gpointer tagsistant_deduplication_loop(gpointer data) {
	(void) data;

	while (1) {
		/* get a path from the queue */
		gchar *path = (gchar *) g_async_queue_pop(tagsistant_deduplication_queue);

		/* process the path only if it's not null */
		if (path && strlen(path)) {
			dbg('2', LOG_ERR, "Starting parallel deduplication of %s", path);
			tagsistant_deduplication_kernel(path);
		} else {
			dbg('2', LOG_ERR, "NULL or zero-length path scheduled for deduplication");
		}

		/* throw away the path */
		g_free_null(path);
	}

	return (NULL);
}
#endif

/**
 * This is the loop that calls the autotagging thread
 * when TAGSISTANT_AUTOTAG_IN_MULTIPLE_THREADS is 0
 */
gpointer tagsistant_autotagging_loop(gpointer data) {
	(void) data;

	while (1) {
		/* get a path from the queue */
		gchar *path = (gchar *) g_async_queue_pop(tagsistant_autotagging_queue);

		/* process the path only if it's not null */
		if (path && strlen(path)) tagsistant_autotagging_kernel(path);

		/* throw away the path */
		g_free_null(path);
	}

	return (NULL);
}

/**
 * Callback for tagsistant_fix_checksums()
 */
int tagsistant_fix_checksums_callback(void *null_pointer, dbi_result result)
{
	(void) null_pointer;

	/* fetch the inode and the objectname from the query */
	const gchar *inode = dbi_result_get_string_idx(result, 1);
	const gchar *objectname = dbi_result_get_string_idx(result, 2);

	/* build the path using the ALL/ tag */
	gchar *path = g_strdup_printf("/store/ALL/@@/%s%s%s", inode, TAGSISTANT_INODE_DELIMITER, objectname);

	/* deduplicate the object */
	tagsistant_deduplicate(path);

	/* free the path and return */
	g_free(path);

	return (0);
}

/**
 * called once() after starting to fix object entries lacking the checksum
 */
void tagsistant_fix_checksums()
{
	/* get a dedicated connection */
	dbi_conn *dbi = tagsistant_db_connection(0);

	/*
	 * find all the objects without a checksum. the inode is cast to varchar(12)
	 * to simplify the callback function
	 */
	tagsistant_query(
		"select cast(inode as char(12)), objectname from objects where checksum = '' and (symlink = '' or symlink is null)",
		dbi, tagsistant_fix_checksums_callback, NULL);

	tagsistant_db_connection_release(dbi, 1);
}

/**
 * Setup deduplication thread and facilities
 */
void tagsistant_deduplication_init()
{
#if ! TAGSISTANT_INLINE_DEDUPLICATION

	/* setup the deduplication queue */
	tagsistant_deduplication_queue = g_async_queue_new_full(g_free);
	g_async_queue_ref(tagsistant_deduplication_queue);

	/* start the deduplication thread */
	g_thread_new("Deduplication thread", tagsistant_deduplication_loop, NULL);
#endif

	/* setup the autotagging queue */
	tagsistant_autotagging_queue = g_async_queue_new_full(g_free);
	g_async_queue_ref(tagsistant_autotagging_queue);

	/* start the autotagging thread */
	g_thread_new("Autotagging thread", tagsistant_autotagging_loop, NULL);

	/* fix missing checksums */
	tagsistant_fix_checksums();
}

/**
 * deduplicate an object
 *
 * @path the path to be deduplicated
 */
void tagsistant_deduplicate(const gchar *path)
{
#if TAGSISTANT_INLINE_DEDUPLICATION
	dbg('2', LOG_ERR, "Inline deduplication of %s", path);
	tagsistant_deduplication_kernel(g_strdup(path));
#else
	dbg('2', LOG_ERR, "Scheduling deduplication of %s", path);
	g_async_queue_push(tagsistant_deduplication_queue, g_strdup(path));
#endif
}
