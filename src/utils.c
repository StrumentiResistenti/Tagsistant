/*
   Tagsistant (tagfs) -- utils.c
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

#ifdef DEBUG_TO_LOGFILE
void open_debug_file()
{
	char debug_file[1024];
	sprintf(debug_file, "/tmp/tagsistant.debug.%d", getpid());
	tagsistant.debugfd = fopen(debug_file, "w");
	if (tagsistant.debugfd is NULL)
		dbg('l', LOG_ERR, "Can't open logfile %s: %s!", debug_file, strerror(errno));
}
#endif

#ifdef _DEBUG_SYSLOG
/**
 * initialize syslog stream
 */
void tagsistant_init_syslog()
{
	static int enabled = 0;
	if (!enabled) {
		openlog("tagsistant", LOG_PID, LOG_DAEMON);
		enabled = 1;
	}
}
#endif

#ifdef MACOSX
ssize_t getline(char **lineptr, size_t *n, FILE *stream)
{
	if (*lineptr is NULL)
		*lineptr = g_malloc0(sizeof(char) * (*n + 1));

	if (*lineptr is NULL)
		return(0);

	if (fgets(*lineptr, *n, stream) is NULL)
		*n = 0;
	else
		*n = strlen(*lineptr);

	return(*n);
}
#endif

/**
 * Print configuration lines on STDERR
 */
void tagsistant_show_config()
{
	int c;

	// repo internal data
	fprintf(stderr, "\n[Repository]\n");
	c = 1;
	fprintf(stderr, "repository: %s\n", tagsistant.repository);
	fprintf(stderr, "archive: %s\n", tagsistant.archive);
	fprintf(stderr, "mount_point: %s\n", tagsistant.mountpoint);

	// SQL backend
	fprintf(stderr, "\n[SQL]\n");
	fprintf(stderr, "db_options: %s\n", tagsistant.dboptions);
	dbi_driver driver = NULL;
	c = 1;
#if TAGSISTANT_REENTRANT_DBI
	while ((driver = dbi_driver_list_r(driver, tagsistant.dbi_instance))) {
#else
	while ((driver = dbi_driver_list(driver))) {
#endif
		fprintf(stderr, "driver_%02d: %s, %s\n", c++, dbi_driver_get_name(driver), dbi_driver_get_filename(driver));
	}

	// plugin infrastructure
	fprintf(stderr, "\n[Plugins]\n");
	tagsistant_plugin_t *pp = tagsistant.plugins;
	c = 1;
	while (pp isNot NULL) {
		fprintf(stderr, "%s: %s\n", pp->mime_type, pp->filename);
		pp = pp->next;
	}
}

/**
 * Create an object and tag it
 *
 * @param qtree the querytree asking object creation
 * @param tagsistant_errno error_reporting variable
 * @param force_create boolean: if true, creation is forced
 */
int
tagsistant_inner_create_and_tag_object(
	tagsistant_querytree *qtree,
	int *tagsistant_errno,
	int force_create)
{
	tagsistant_inode inode = 0;

	/*
	 * 1. create the object on db or get its inode if exists
	 *    if force_create is true, create a new object and
	 *    fetch its inode if force_create is false, try to
	 *    find an object with name and path matching
	 *    and use its inode, otherwise create a new one
	 */
	if (!force_create) {
		tagsistant_query(
			"select inode from objects where objectname = '%s' limit 1",
			qtree->dbi,
			tagsistant_return_integer,
			&inode,
			qtree->object_path);
	}

	if (force_create || (!inode)) {
		tagsistant_query(
			"insert into objects (objectname) values ('%s')",
			qtree->dbi, NULL, NULL, qtree->object_path);

		inode = tagsistant_last_insert_id(qtree->dbi);
	}

	if (!inode) {
		dbg('F', LOG_ERR, "Object %s recorded as inode 0!", qtree->object_path);
		*tagsistant_errno = EIO;
		return(-1);
	}

	/*
	 *  2. adjust archive_path and full_archive_path with leading inode
	 */
	tagsistant_querytree_set_inode(qtree, inode);

	/*
	 *  3. tag the object
	 */
	tagsistant_querytree_traverse(qtree, tagsistant_sql_tag_object, inode);

	if (force_create) {
		dbg('l', LOG_INFO, "Forced creation of object %s", qtree->full_path);
	} else {
		dbg('l', LOG_INFO, "Tried creation of object %s", qtree->full_path);
	}

	return(inode);
}

#if 0 && TAGSISTANT_ENABLE_AUTOTAGGING && TAGSISTANT_ENABLE_AUTOTAGGING_THREAD
extern GThread *tagsistant_autotag_thread;
extern GAsyncQueue *tagsistant_autotag_queue;
extern void tagsistant_autotag_thread_kernel(gpointer data);
#endif

GMutex tagsistant_tags_list_mutex;
GRegex *tagsistant_tags_list_rx = NULL;
GRegex *tagsistant_tags_list_removal_rx = NULL;

/**
 * Initialize all the utilities
 */
void tagsistant_utils_init()
{
	/*
	 * transform the tag suffix command line option into a
	 * regular expression anchored to the end of the line
	 * and then create a GRegex object using that pattern
	 */
	gchar *escaped = g_regex_escape_string(tagsistant.tags_suffix, -1);
	gchar *pattern = g_strdup_printf("%s/[^/]*%s$", TAGSISTANT_QUERY_DELIMITER, escaped);

	dbg('l', LOG_INFO, "tag-suffix detection regex: %s", pattern);

	tagsistant_tags_list_rx = g_regex_new(pattern, G_REGEX_OPTIMIZE|G_REGEX_DOLLAR_ENDONLY, 0, NULL);
	g_free(pattern);

	/*
	 * transform the tag suffix command line option into a
	 * regular expression to remove the suffix
	 */
	pattern = g_strdup_printf("%s$", escaped);

	dbg('l', LOG_INFO, "tag-suffix removal regex: %s", pattern);

	tagsistant_tags_list_removal_rx = g_regex_new(pattern, G_REGEX_OPTIMIZE|G_REGEX_DOLLAR_ENDONLY, 0, NULL);

	g_free(pattern);
	g_free(escaped);
}

/**
 * guess if a filename refers to a tag-listing special file or not
 *
 * @param qtree the tagsistant_querytree object describing the filename
 * @return true if the file is a tag-listing special file, false otherwise
 */
gboolean tagsistant_is_tags_list_file(tagsistant_querytree *qtree)
{
	g_mutex_lock(&tagsistant_tags_list_mutex);

	/* the file must be taggable (this is the path must end one token after @/ or @@/ */
	/* the file must end by the tag-listing suffix (default: .tags) */
	if (g_regex_match(tagsistant_tags_list_rx, qtree->full_path, 0, NULL)) {
		g_mutex_unlock(&tagsistant_tags_list_mutex);
		return (TRUE);
	}

	g_mutex_unlock(&tagsistant_tags_list_mutex);
	return (FALSE);
}

/**
 * removes the tag-suffix from a filename
 *
 * @param qtree the tagsistant_querytree object describing the filename
 * @return true if the file is a tag-listing special file, false otherwise
 */
gchar *tagsistant_string_tags_list_suffix(tagsistant_querytree *qtree)
{
	g_mutex_lock(&tagsistant_tags_list_mutex);
	gchar *stripped = g_regex_replace_literal(tagsistant_tags_list_removal_rx, qtree->full_path, -1, 0, "", 0, NULL);
	g_mutex_unlock(&tagsistant_tags_list_mutex);

	return (stripped);
}
/****************************************************************************/
/***                                                                      ***/
/***   Repository .ini file parsing and writing                           ***/
/***                                                                      ***/
/****************************************************************************/

GKeyFile *tagsistant_ini = NULL;

#define tagsistant_get_repository_ini_path() g_strdup_printf("%s/repository.ini", tagsistant.repository)

/**
 * Read the repository.ini file contained into a tagsistant repository
 *
 * @return a GKeyFile object
 */
GKeyFile *tagsistant_parse_repository_ini()
{
	GError *error = NULL;
	gchar *ini_path = tagsistant_get_repository_ini_path();
	GKeyFile *kf = g_key_file_new();

	// load the key file
	g_key_file_load_from_file(kf, ini_path, G_KEY_FILE_NONE, &error);
	g_free_null(ini_path);

	// if no error occurred, return the GKeyFile object
	if (!error) return (kf);

	// otherwise, free the GKeyFile object and return NULL
	g_key_file_free(kf);
	return (NULL);
}

/**
 * Save a repository.ini file
 *
 * @param kf the GKeyFile object to save
 */
void tagsistant_save_repository_ini(GKeyFile *kf)
{
	gchar *ini_path = tagsistant_get_repository_ini_path();
	gsize size = 0;
	gchar *content = g_key_file_to_data(kf, &size, NULL);

	// open the file and write the content
	int fd = open(ini_path, O_WRONLY|O_CREAT|O_TRUNC, S_IRUSR|S_IWUSR);
	if (fd isNot -1) {
		int written = write(fd, content, size);
		if (written is -1) {
			dbg('l', LOG_ERR, "Error writing %s: %s", ini_path, strerror(errno));
		}
		close(fd);
	} else {
		dbg('l', LOG_ERR, "Unable to write %s: %s", ini_path, strerror(errno));
	}

	g_free_null(content);
	g_free_null(ini_path);
}

#define tagsistant_set_init_default(kf, section, key, value) \
	if (!g_key_file_has_key(kf, section, key, NULL))\
		g_key_file_set_value(kf, section, key, value);

/**
 * Read the repository.ini file, compare its content with
 * provided command line arguments and than saves back the
 * merge of both in repository.ini
 */
void tagsistant_manage_repository_ini()
{
	// read the repository.ini file from disk
	tagsistant_ini = tagsistant_parse_repository_ini();

	if (tagsistant_ini) {
		if (g_key_file_has_group(tagsistant_ini, "Tagsistant")) {
			if (g_key_file_has_key(tagsistant_ini, "Tagsistant", "db", NULL)) {
				if (tagsistant.dboptions) {
					dbg('b', LOG_INFO, "Ignoring command line --db parameter in favor of repository.ini");
				}
				tagsistant.dboptions = g_key_file_get_value(tagsistant_ini, "Tagsistant", "db", NULL);
			}
		}
	}

	// if repository.ini has not been laded, create an empty GKeyFile object
	if (!tagsistant_ini) tagsistant_ini = g_key_file_new();

	// fill the GKeyFile object with command line values
	if (!tagsistant.dboptions) tagsistant.dboptions = g_strdup("sqlite3::::");
	if (!strlen(tagsistant.dboptions)) {
		g_free_null(tagsistant.dboptions);
		tagsistant.dboptions = g_strdup("sqlite3::::");
	}

	g_key_file_set_value(tagsistant_ini, "Tagsistant", "db", tagsistant.dboptions);
	g_key_file_set_value(tagsistant_ini, "Tagsistant", "mountpoint", tagsistant.mountpoint);
	g_key_file_set_value(tagsistant_ini, "Tagsistant", "repository", tagsistant.repository);

	// set default plugin filters
	tagsistant_set_init_default(tagsistant_ini, "mime:application/xml",	"filter", "^(author|date|language)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:image/gif",		"filter", "^(size|orientation)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:text/html",		"filter", "^(author|date|language)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:image/jpeg",		"filter", "^(size|orientation)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:image/png",		"filter", "^(size|orientation)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:application/ogg",	"filter", "^(year|album|artist)$");
	tagsistant_set_init_default(tagsistant_ini, "mime:audio/mpeg",		"filter", "^(year|album|artist)$");

	// save and free the GKeyFile object
	tagsistant_save_repository_ini(tagsistant_ini);
}

/**
 * Transform flat archives into hierarchical archives
 */
void tagsistant_fix_archive()
{
	DIR *dir = opendir(tagsistant.archive);
	if (!dir) return;

	/*
	 * scan the archive directory
	 */
	struct dirent *dirent;
	while ((dirent = readdir(dir)) isNot NULL) {

		/*
		 * for each file call lstat() to check if it's a regular file
		 */
		gchar *filename = g_strdup_printf("%s/%s", tagsistant.archive, dirent->d_name);
		struct stat st;
		int res = lstat(filename, &st);

		/*
		 * if it's a file...
		 */
		if ((res is 0) && (S_ISREG(st.st_mode))) {
			GMatchInfo *match_info;
			GRegex *rx = g_regex_new("([0-9]+)" TAGSISTANT_INODE_DELIMITER, 0, 0, NULL);

			/*
			 * look for a trailing inode...
			 */
			g_regex_match(rx, dirent->d_name, 0, &match_info);
			if (g_match_info_matches (match_info)) {

				/*
				 * extract its inode string...
				 */
				gchar *inode_string = g_match_info_fetch (match_info, 1);
				tagsistant_inode inode = atoi(inode_string);
				g_free(inode_string);

				/*
				 * compute the reverse inode and the the directory tree
				 */
				gchar *tree = tagsistant_get_reversed_inode_tree(inode);
				gchar *full_tree = g_strdup_printf("%s/%s", tagsistant.archive, tree);
				int res = mkdir(full_tree, 0755);

				if (0 isNot res && EEXIST isNot errno) {
					dbg('b', LOG_ERR,  "Error creating directory %s", full_tree);
				} else {
					/*
					 * move the file inside the directory
					 */
					gchar *new_name = g_strdup_printf("%s/%s/%s", tagsistant.archive, tree, dirent->d_name);
					rename(filename, new_name);
					g_free(new_name);
				}

				g_free(tree);
				g_free(full_tree);
			}
			g_match_info_free (match_info);
			g_regex_unref(rx);
		}

		g_free(filename);
	}

	closedir(dir);
}

int tagsistant_read_file_tags(void *tagsbuffer, dbi_result result)
{
	GString *buffer = (GString *) tagsbuffer;

	const gchar *next_tag = dbi_result_get_string_idx(result, 1);

	if (g_regex_match_simple(tagsistant.triple_tag_regex, next_tag, 0, 0)) {
		g_string_append_printf(buffer, "%s%s=%s\n",
			next_tag,
			dbi_result_get_string_idx(result, 2),
			dbi_result_get_string_idx(result, 3));
	} else {
		g_string_append_printf(buffer, "%s\n", next_tag);
	}

	return (1);
}

gchar *tagsistant_get_file_tags(tagsistant_querytree *qtree)
{
	/* strip the suffix from the path */
	gchar *stripped_path = tagsistant_string_tags_list_suffix(qtree);
	
	/* create a new one on the stripped path */
	tagsistant_querytree *stripped_qtree = tagsistant_querytree_new(stripped_path, 0, 0, 1, 1);
	
	/* free the stripped path */
	g_free(stripped_path);
	
	if (!stripped_qtree->inode) return (NULL);
	
	/* allocate a buffer GString and fill it with the tags bound to the file */
	GString *tagsbuffer = g_string_sized_new(1024);
	
	tagsistant_query(
		"select tagname, `key`, value from tags "
			"join tagging on tagging.tag_id = tags.tag_id "
			"where tagging.inode = %d",
		stripped_qtree->dbi,
		tagsistant_read_file_tags,
		(void *) tagsbuffer,
		stripped_qtree->inode);
	
	gchar *tags_list = tagsbuffer->str;

	/* free the GString but keep the buffer */
	g_string_free(tagsbuffer, 0);
	tagsistant_querytree_destroy(stripped_qtree, TAGSISTANT_ROLLBACK_TRANSACTION);

	return (tags_list);
}
