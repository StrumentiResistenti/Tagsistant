/*
   Tagsistant (tagfs) -- tp_filename.c
   Copyright (C) 2014-2015 Rouven Rastetter <rouven.rastetter@firaweb.de>

   Tagsistant filename plugin which tags files by filename.

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

/*
sample config:
[filename]
filter=\.(DIRECTORSCUT|EXTENDED|UNCUT|1080p|720p|\d{4}|CUT|ENG|FR|DL|HD)
simple=DIRECTORSCUT,EXTENDED,UNCUT,1080p,720p,CUT,ENG,FR,DL,HD
machine=time:year:\d{4}
splitter=,

sample filename:
Stargate.DIRECTORSCUT.1080p.DL.1994.mkv
 */

#include "../tagsistant.h"

typedef struct machine_tag {
	struct machine_tag *next;
	gchar **tags;
	gchar *namespace;
	gchar *keyword;
	GRegex *rx;
} machine_tag;

/* declaring mime type */
char mime_type[] = "*/*";

const gchar m_splitter[] = ":";

/* the regular expression used to match the tags to be considered */
GRegex *rx = NULL;
gchar **simple_tags_begin = NULL;
gchar **simple_tags_current = NULL;
machine_tag *machine_tags_begin = NULL;
machine_tag *machine_tags_current = NULL;

gboolean simple_active = TRUE;
gboolean machine_active = TRUE;
gboolean splitter_free = TRUE;

/* exported init function */
int tagsistant_plugin_init()
{
	/* get the config parameters from the .ini file */
	gchar *pattern = tagsistant_get_ini_entry("filename", "filter");
	gchar *simple = tagsistant_get_ini_entry("filename", "simple");
	gchar *machine = tagsistant_get_ini_entry("filename", "machine");
	gchar *splitter = tagsistant_get_ini_entry("filename", "splitter");

	/* default splitter */
	if (!splitter || g_strcmp0(splitter, "") == 0) {
		if(g_strcmp0(splitter, "") == 0) {
			g_free(splitter);
		}
		splitter = ",";
		splitter_free = FALSE;
	}

	if (!simple || g_strcmp0(simple, "") == 0) {
		simple_active = FALSE;
	}

	if (!machine || g_strcmp0(machine, "") == 0) {
		machine_active = FALSE;
	}

	/* disable plugin if there are two few settings for it */
	if (!pattern || (g_strcmp0(pattern, "") == 0) || (!simple_active && !machine_active)) {
		dbg('p', LOG_INFO, "filename: disabled");

		if (splitter_free) {
			g_free(splitter);
		}
		g_free(machine);
		g_free(simple);
		g_free(pattern);
		return(0);
	}

	/* prepare the regular expression */
	GError *err = NULL;
	rx = g_regex_new(pattern, TAGSISTANT_RX_COMPILE_FLAGS, 0, &err);

	/* disable plugin on regex error */
	if (err != NULL) {
		dbg('p', LOG_ERR, "filename: %s", err->message);
		g_error_free(err);
		g_regex_unref(rx);
		if (splitter_free) {
			g_free(splitter);
		}
		g_free(machine);
		g_free(simple);
		g_free(pattern);
		return(0);
	}

	/* split the config parameters into tags */
	if(simple_active) {
		simple_tags_begin = g_strsplit(simple, splitter, 0); // FIXME: check for syntax errors in simple_tags_begin
	}

	if(machine_active) {
		gchar **split_m_tags = g_strsplit(machine, splitter, 0);
		int i = 0;
		while(split_m_tags[i] != NULL) { // store the machine tag components in an easy access struct
			if(g_strcmp0(split_m_tags[i], "") == 0) { // FIXME: check for more syntax errors in split_m_tags[i]
				continue;
			}
			machine_tags_current = g_new(machine_tag, 1);
			if(machine_tags_begin == NULL) {
				machine_tags_begin = machine_tags_current; // initialize machine_tags_begin
			}
			machine_tags_current->tags = g_strsplit(split_m_tags[i], m_splitter, 0);
			machine_tags_current->namespace = machine_tags_current->tags[0];
			machine_tags_current->keyword = machine_tags_current->tags[1];
			machine_tags_current->rx = g_regex_new(machine_tags_current->tags[2], TAGSISTANT_RX_COMPILE_FLAGS, 0, &err);
			if (err != NULL) {
				dbg('p', LOG_ERR, "filename: machine-tag: %s", err->message);
				g_error_free(err);
				g_regex_unref(machine_tags_current->rx);
				machine_tags_current->rx = NULL;
			}
			i++;
		}
		machine_tags_current->next = NULL; // terminate list with NULL
		g_strfreev(split_m_tags);
	}
	if (splitter_free) {
		g_free(splitter);
	}
	g_free(machine);
	g_free(simple);
	g_free(pattern);

	return(1);
}

/* exported processor function */
int tagsistant_processor(tagsistant_querytree *qtree, tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS])
{
	gchar *filename = g_strdup(qtree->object_path);
	GMatchInfo *match_info;
	g_regex_match(rx, filename, 0, &match_info);
	gboolean is_simple = FALSE;

	if(!g_match_info_matches(match_info)) {
		g_match_info_free(match_info);
		return(TP_NULL);
	}

	do {
		gchar *match = g_match_info_fetch(match_info, 1);
		if(simple_active) {
			is_simple = FALSE;
			simple_tags_current = simple_tags_begin;
			while((*simple_tags_current) != NULL) {
				if(g_strcmp0(*simple_tags_current, match) == 0) {
					tagsistant_sql_tag_object(qtree->dbi, match, NULL, NULL, qtree->inode);
					is_simple = TRUE;
					break;
				}
				simple_tags_current++;
			}

		}
		if(machine_active && !is_simple) {
			GMatchInfo *machine_match_info;
			machine_tags_current = machine_tags_begin;
			while(machine_tags_current != NULL) {
				if(machine_tags_current->rx == NULL) {
					continue;
				}
				g_regex_match(machine_tags_current->rx, match, 0, &machine_match_info);
				if(g_match_info_matches(machine_match_info)) {
					gchar *namespace = g_strconcat(machine_tags_current->namespace, ":\0", NULL);
					tagsistant_sql_tag_object(qtree->dbi, namespace, machine_tags_current->keyword, match, qtree->inode);
					g_free(namespace);
					g_match_info_free(machine_match_info);
					break;
				}
				g_match_info_free(machine_match_info);
				machine_tags_current = machine_tags_current->next;
			}
		}
		// remove matched tag from filename
		gchar *filename_search = g_strrstr(filename, match);
		gchar *filename_temp = g_strndup(filename, strlen(filename) - strlen(match));
		int pos = (int)(filename_search - filename);
		filename_search += strlen(match);
		while((*filename_search) != '\0') {
			filename_temp[pos++] = *filename_search;
			filename_search++;
		}
		g_free(filename);
		filename = filename_temp;

		g_free(match);
		g_match_info_free(match_info);

		g_regex_match(rx, filename, 0, &match_info);
	} while (g_match_info_matches(match_info));

	return(TP_OK);
}

/* exported finalize function */
void tagsistant_plugin_free()
{
	g_regex_unref(rx);

	if(simple_active) {
		g_strfreev(simple_tags_begin);
	}

	/* free the machine_tags array */
	if(machine_active) {
		machine_tags_current = machine_tags_begin;
		machine_tag *m_tags_temp = machine_tags_begin;
		while(machine_tags_current != NULL) {
			g_strfreev(machine_tags_current->tags);
			if(machine_tags_current->rx != NULL) {
				g_regex_unref(machine_tags_current->rx);
			}
			m_tags_temp = machine_tags_current->next;
			g_free(machine_tags_current);
			machine_tags_current = m_tags_temp;
		}
	}
}
