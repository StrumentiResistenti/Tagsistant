/*
   Tagsistant (tagfs) -- plugin.h
   Copyright (C) 2006-2013 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) mount binary written using FUSE userspace library.
   Header file

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

/* codes used in plugin chain processing */

#define TP_ERROR	0	/**< an error occurred while processing with this plugin */
#define TP_OK		1	/**< ok, but further tagging can be done by other plugins */
#define TP_STOP		2	/**< this plugin is authoritative for mimetype, stop chaining */
#define TP_NULL		3	/**< no tagging has been done, but that's not an error */

/* flags suggested to compile regular expressions passed to tagsistant_plugin_iterator */
#define TAGSISTANT_RX_COMPILE_FLAGS G_REGEX_CASELESS|G_REGEX_EXTENDED|G_REGEX_OPTIMIZE

#define TAGSISTANT_MAX_KEYWORDS 1024
#define TAGSISTANT_MAX_KEYWORD_LENGTH 256

typedef struct {
	gchar keyword[TAGSISTANT_MAX_KEYWORD_LENGTH];
	gchar value[TAGSISTANT_MAX_KEYWORD_LENGTH];
} tagsistant_keyword;

/**
 * holds a pointer to a processing function
 * exported by a plugin
 */
typedef struct tagsistant_plugin {
	/** MIME type managed by this plugins */
	char *mime_type;

	/** file name of this plugin */
	char *filename;

	/** handle to plugin returned by dlopen() */
	void *handle;

	/**
	 * hook to processing function
	 *
	 * @param qtree the querytree object
	 * @param keywords an array of tagsistant_keyword to be applied to the qtree object
	 * @return 0 on failure (the plugin wasn't unable to process the file), 1 on
	 *   partial success (the plugin did processed the file, but later processing
	 *   by other plugins is allowed) or 2 on successful processing (no further
	 *   processing required).
	 */
	int (*processor)(tagsistant_querytree *qtree, tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS]);

	/**
	 * hook to g_free allocated resources
	 */
	void (*free)();

	/** next plugin in linked list */
	struct tagsistant_plugin *next;
} tagsistant_plugin_t;

extern void tagsistant_plugin_iterator(
	const tagsistant_querytree *qtree,
	const gchar *namespace,
	tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS],
	GRegex *regex);

extern const gchar *tagsistant_plugin_get_keyword_value(
	gchar *keyword,
	tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS]);

extern void tagsistant_plugin_tag_by_date(const tagsistant_querytree *qtree, const gchar *date);
