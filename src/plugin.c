/*
   Tagsistant (tagfs) -- plugin.c
   Copyright (C) 2006-2016 Tx0 <tx0@strumentiresistenti.org>

   Tagsistant (tagfs) plugin support

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

/******************\
 * PLUGIN SUPPORT *
\******************/

#if TAGSISTANT_EXTRACTOR is 5
static EXTRACTOR_ExtractorList *elist;
#else
static struct EXTRACTOR_PluginList *plist;
#endif

static GRegex *tagsistant_rx_date;
static GRegex *tagsistant_rx_cleaner;

#ifndef errno
#define errno
#endif

/*
 * This mutex is used to concurrently access libextractor facilities
 */
GMutex tagsistant_processor_mutex;

/**
 * run the processor function of the passed plugin
 *
 * @param plugin the plugin to apply
 * @param qtree the querytree object to be scanned by the plugin
 * @param keywords the set of keywords pre-extracted by libextractor
 * @return
 */
int tagsistant_run_processor(
	tagsistant_plugin_t *plugin,
	tagsistant_querytree *qtree,
	tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS],
	int keyword_counter)
{
	/* call plugin processor */
	dbg('p', LOG_INFO, "Applying plugin %s", plugin->filename);
	int res = (plugin->processor)(qtree, keywords, keyword_counter);

	/* report about processing */
	switch (res) {
		case TP_ERROR:
			dbg('p', LOG_ERR, "Plugin %s was supposed to apply to %s, but failed!", plugin->filename, qtree->full_archive_path);
			break;
		case TP_OK:
			dbg('p', LOG_INFO, "Plugin %s tagged %s", plugin->filename, qtree->full_archive_path);
			break;
		case TP_STOP:
			dbg('p', LOG_INFO, "Plugin %s stopped chain on %s", plugin->filename, qtree->full_archive_path);
			break;
		case TP_NULL:
			dbg('p', LOG_INFO, "Plugin %s did not tagged %s", plugin->filename, qtree->full_archive_path);
			break;
		default:
			dbg('p', LOG_ERR, "Plugin %s returned unknown result %d", plugin->filename, res);
			break;
	}

	return (res);
}

#if TAGSISTANT_EXTRACTOR is 5

/**
 * process a file using plugin chain
 *
 * @param filename file to be processed (just the name, will be looked up in /archive)
 * @return zero on fault, one on success
 */
int tagsistant_process(gchar *path, gchar *full_archive_path)
{
	int res = 0;
	gchar *mime_type = NULL;
	gchar *mime_generic = NULL;
	tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS];

	/* blank the keyword buffer */
	memset(keywords, 0, TAGSISTANT_MAX_KEYWORDS * 2 * TAGSISTANT_MAX_KEYWORD_LENGTH);

	dbg('p', LOG_INFO, "Processing file %s", full_archive_path);

	/* lock processor mutex */
	g_mutex_lock(&tagsistant_processor_mutex);

	/*
	 * Extract the keywords and remove duplicated ones
	 */
	EXTRACTOR_KeywordList *extracted_keywords = EXTRACTOR_getKeywords(elist, full_archive_path);
	extracted_keywords = EXTRACTOR_removeDuplicateKeywords (extracted_keywords, 0);

	/*
	 * create the querytree object from the path
	 */
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 1, 1, 0);

	/*
	 *  loop through the keywords and feed the keyword buffer
	 */
	EXTRACTOR_KeywordList *keyword_pointer = extracted_keywords;
	int c = 0;
	while (keyword_pointer && c < TAGSISTANT_MAX_KEYWORDS) {
		sprintf(keywords[c].keyword, "%s", EXTRACTOR_getKeywordTypeAsString(keyword_pointer->keywordType));
		sprintf(keywords[c].value, "%s", keyword_pointer->keyword);

		/* save the mime type */
		if (keyword_pointer->keywordType is EXTRACTOR_MIMETYPE) {
			// mime_type = EXTRACTOR_getKeywordValueAsString(keyword_pointer->keywordType);
			mime_type = g_strdup(keyword_pointer->keyword);
		}

		keyword_pointer = keyword_pointer->next;
		c++;
	}

	/*
	 * If no mime type has been found just return
	 */
	if (!mime_type) {
		/* lock processor mutex */
		g_mutex_unlock(&tagsistant_processor_mutex);
		tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
		return(res);
	}

	/*
	 * guess the generic MIME type
	 */
	mime_generic = g_strdup(mime_type);
	gchar *slash = index(mime_generic, '/');
	if (slash) {
		slash++; *slash = '*';
		slash++; *slash = '\0';
	}

	/*
	 *  apply plugins starting from the most matching first (like: image/jpeg)
	 */
	tagsistant_plugin_t *plugin = tagsistant.plugins;
	while (plugin isNot NULL) {
		if (strcmp(plugin->mime_type, mime_type) is 0) {
			if (tagsistant_run_processor(plugin, qtree, keywords, TAGSISTANT_MAX_KEYWORDS - 1) is TP_STOP){
				goto STOP_CHAIN_TAGGING;
			}
		}
		plugin = plugin->next;
	}

	/*
	 * mime generic then (like: image / *)
	 */
	plugin = tagsistant.plugins;
	while (plugin isNot NULL) {
		if (strcmp(plugin->mime_type, mime_generic) is 0) {
			if (tagsistant_run_processor(plugin, qtree, keywords, TAGSISTANT_MAX_KEYWORDS - 1) is TP_STOP) {
				goto STOP_CHAIN_TAGGING;
			}
		}
		plugin = plugin->next;
	}

	/*
	 * mime everything (* / *)
	 */
	plugin = tagsistant.plugins;
	while (plugin isNot NULL) {
		if (strcmp(plugin->mime_type, "*/*") is 0) {
			if (tagsistant_run_processor(plugin, qtree, keywords, TAGSISTANT_MAX_KEYWORDS - 1) is TP_STOP) {
				goto STOP_CHAIN_TAGGING;
			}
		}
		plugin = plugin->next;

		dbg('p', LOG_INFO, "Using generic plugin on MIME type %s", mime_type);
	}

STOP_CHAIN_TAGGING:

	g_free_null(mime_type);
	g_free_null(mime_generic);

	dbg('p', LOG_INFO, "Processing of %s ended.", qtree->full_archive_path);

	tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);

	/* free the keyword structure */
	EXTRACTOR_freeKeywords(extracted_keywords);

	/* lock processor mutex */
	g_mutex_unlock(&tagsistant_processor_mutex);

	return(res);
}

#else

#define TAGSISTANT_MIME_TYPE_FIELD_LENGTH 1024

typedef struct {
	tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS];
	int current_keyword;
	gchar mime_type[TAGSISTANT_MIME_TYPE_FIELD_LENGTH];
	gchar generic_mime_type[TAGSISTANT_MIME_TYPE_FIELD_LENGTH];
	tagsistant_querytree *qtree;
} tagsistant_process_callback_context;

static int tagsistant_process_callback(
	void *cls, const char *plugin_name, enum EXTRACTOR_MetaType type,
	enum EXTRACTOR_MetaFormat format, const char *data_mime_type,
	const char *data, size_t data_len)
{
	(void) plugin_name;
	(void) format;
	(void) data_mime_type;

	tagsistant_process_callback_context *context = (tagsistant_process_callback_context *) cls;
	int res = 0;

	/* copy the keyword and its value into the context keywords buffer */
	if (context->current_keyword < TAGSISTANT_MAX_KEYWORDS) {
		sprintf(context->keywords[context->current_keyword].keyword, "%s", EXTRACTOR_metatype_to_string(type));
		memcpy(context->keywords[context->current_keyword].value, data, data_len);
		context->current_keyword += 1;
	}

	/* save the mime type */
	if (type is EXTRACTOR_METATYPE_MIMETYPE) {
		memset(context->mime_type, 0, TAGSISTANT_MIME_TYPE_FIELD_LENGTH);
		memcpy(context->mime_type, data, data_len);

		/* guess the generic MIME type */
		memset(context->generic_mime_type, 0, TAGSISTANT_MIME_TYPE_FIELD_LENGTH);
		memcpy(context->generic_mime_type, data, data_len);

		gchar *slash = index(context->generic_mime_type, '/');
		if (slash) {
			slash++; *slash = '*';
			slash++; *slash = '\0';
		}
	}

	return (res);
}

/**
 * process a file using plugin chain
 *
 * @param filename file to be processed (just the name, will be looked up in /archive)
 * @return(zero on fault, one on success)
 */
int tagsistant_process(gchar *path, gchar *full_archive_path)
{
	int res = 0;

	dbg('p', LOG_INFO, "Processing file %s", full_archive_path);

	tagsistant_process_callback_context context;
	memset(context.keywords, 0, TAGSISTANT_MAX_KEYWORDS * TAGSISTANT_MAX_KEYWORD_LENGTH * 2);
	memset(context.mime_type, 0, TAGSISTANT_MIME_TYPE_FIELD_LENGTH);
	memset(context.generic_mime_type, 0, TAGSISTANT_MIME_TYPE_FIELD_LENGTH);
	context.current_keyword = 0;

	/*
	 * recreate the querytree object just before using it to tag the object
	 */
	tagsistant_querytree *qtree = tagsistant_querytree_new(path, 0, 1, 1, 0);
	if (!qtree) goto STOP_CHAIN_TAGGING;

	/*
	 * Extract the keywords
	 */
	context.qtree = qtree;
	EXTRACTOR_extract(plist, full_archive_path, NULL, 0, tagsistant_process_callback, (void *) &context);

	/*
	 * If no mime type has been found, set the most generic available:
	 * application/octet-stream.
	 */
	gchar default_mimetype[] = "application/octet-stream";
	if (!strlen(context.mime_type)) strcpy(context.mime_type, default_mimetype);

	/*
	 *  apply plugins starting from the most matching first (like: image/jpeg)
	 */
	tagsistant_plugin_t *plugin = tagsistant.plugins;
	while (plugin isNot NULL) {
		if (strcmp(plugin->mime_type, context.mime_type) is 0) {
			if (tagsistant_run_processor(plugin, qtree, context.keywords, context.current_keyword) is TP_STOP) {
				goto STOP_CHAIN_TAGGING;
			}
		}
		plugin = plugin->next;
	}

	/*
	 * mime generic then (like: image / *)
	 */
	plugin = tagsistant.plugins;
	while (plugin isNot NULL) {
		if (strcmp(plugin->mime_type, context.generic_mime_type) is 0) {
			if (tagsistant_run_processor(plugin, qtree, context.keywords, context.current_keyword) is TP_STOP) {
				goto STOP_CHAIN_TAGGING;
			}
		}
		plugin = plugin->next;
	}

	/*
	 * mime everything (* / *)
	 */
	plugin = tagsistant.plugins;
	while (plugin isNot NULL) {
		if (strcmp(plugin->mime_type, "*/*") is 0) {
			if (tagsistant_run_processor(plugin, qtree, context.keywords, context.current_keyword) is TP_STOP) {
				goto STOP_CHAIN_TAGGING;
			}
		}
		plugin = plugin->next;
	}

STOP_CHAIN_TAGGING:
	tagsistant_querytree_destroy(qtree, TAGSISTANT_COMMIT_TRANSACTION);
	return (res);
}

#endif

/**
 * Apply a tag if a regular expression matches a retrieved keyword
 *
 * @param regex the GRegex regular expression
 * @param keyword a string with the keyword name
 * @param value a string with the keyword value
 * @param qtree the tagsistant_querytree object that could be tagged
 */
void tagsistant_keyword_matcher(
	GRegex *regex,
	const gchar *namespace,
	const gchar *keyword,
	const gchar *value,
	const tagsistant_querytree *qtree)
{
	/*
	 * if the keyword name matches the filter regular expression
	 */
	if (g_regex_match(regex, keyword, 0, NULL)) {

		/*
		 * build a tag which is "keyword_name:keyword_value"
		 */
		size_t keyword_len = strnlen(keyword, TAGSISTANT_MAX_KEYWORD_LENGTH);
		gchar *clean_keyword = g_regex_replace_literal(tagsistant_rx_cleaner, keyword, keyword_len, 0, "-", 0, NULL);

		size_t value_len = strnlen(value, TAGSISTANT_MAX_KEYWORD_LENGTH);
		gchar *clean_value = g_regex_replace_literal(tagsistant_rx_cleaner, value, value_len, 0, "-", 0, NULL);

#if 0
		/* ... turn each slash and space in a dash */
		gchar *tpointer = clean_keyword;
		while (*tpointer) {
			if (*tpointer is '/' || *tpointer is ' ') *tpointer = '-';
			tpointer++;
		}

		tpointer = clean_value;
		while (*tpointer) {
			if (*tpointer is '/' || *tpointer is ' ') *tpointer = '-';
			tpointer++;
		}
#endif

		/*
		 * then tag the file
		 */
		tagsistant_sql_tag_object(qtree->dbi, namespace, clean_keyword, clean_value, qtree->inode);

		/*
		 * and cleanup
		 */
		g_free_null(clean_keyword);
		g_free_null(clean_value);
	} else {
		dbg('p', LOG_INFO, "keyword %s refused by regular expression", keyword);
	}
}

/**
 * Iterate over a set of keywords. For each keyword matching
 * regular expression regex, construct a tag as "keyword_name:keyword_value"
 * and tags the qtree object.
 *
 * @param qtree the querytree object to tag
 * @param keyworkd a EXTRACTOR_KeywordList * list of keywords
 * @param regex a precompiled GRegex object to match against each keyword
 */
void tagsistant_plugin_iterator(
	const tagsistant_querytree *qtree,
	const gchar *namespace,
	tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS],
	int keyword_counter,
	GRegex *regex)
{
	/*
	 * loop through the keywords to tag the file
	 */
	int c = 0;
	for (; c < TAGSISTANT_MAX_KEYWORDS && c < keyword_counter; c++) {
		/* stop looping on the first null keyword */
		if (*(keywords[c].keyword) is '\0') break;

		/* tag the qtree with the keyword if the regular expression matches */
		tagsistant_keyword_matcher(regex, namespace, keywords[c].keyword, keywords[c].value, qtree);
	}
}

/**
 * Return the value of a keyword from a keyword list, if available.
 *
 * @param keyword the keyword to fetch
 * @param keywords the linked list of EXTRACTOR_KeywordList object
 * @return the value of the keyword. This is a pointer to the original value and must not be modified or freed
 */
const gchar *tagsistant_plugin_get_keyword_value(gchar *keyword, tagsistant_keyword keywords[TAGSISTANT_MAX_KEYWORDS])
{
	int c = 0;
	for (; c < TAGSISTANT_MAX_KEYWORDS; c++) {
		if (g_strcmp0(keyword, keywords[c].keyword) is 0) {
			return (keywords[c].value);
		}
	}

	return (NULL);
}

/**
 * Tag a querytree by date.
 *
 * @param qtree the querytree object to tag
 * @param date a constant string in format "YYYY:MM:DD HH:MM:SS"
 */
void tagsistant_plugin_tag_by_date(const tagsistant_querytree *qtree, const gchar *date)
{
	GMatchInfo *match_info;
	GError *error = NULL;

	if (g_regex_match_full(tagsistant_rx_date, date, -1, 0, 0, &match_info, &error)) {
		tagsistant_sql_tag_object(qtree->dbi, "time:", "year",   g_match_info_fetch(match_info, 1), qtree->inode);
		tagsistant_sql_tag_object(qtree->dbi, "time:", "month",  g_match_info_fetch(match_info, 2), qtree->inode);
		tagsistant_sql_tag_object(qtree->dbi, "time:", "day",    g_match_info_fetch(match_info, 3), qtree->inode);
		tagsistant_sql_tag_object(qtree->dbi, "time:", "hour",   g_match_info_fetch(match_info, 4), qtree->inode);
		tagsistant_sql_tag_object(qtree->dbi, "time:", "minute", g_match_info_fetch(match_info, 5), qtree->inode);
//		tagsistant_sql_tag_object(qtree->dbi, "time:", "second", g_match_info_fetch(match_info, 6), qtree->inode);
	}

	g_match_info_unref(match_info);
}

/**
 * Loads the plugins and do other initialization steps
 * like compiling recurring regular expressions
 */
void tagsistant_plugin_loader()
{
#if TAGSISTANT_EXTRACTOR is 5 // libextractor 0.5.x
	elist =  EXTRACTOR_loadDefaultLibraries();
#else
	plist = EXTRACTOR_plugin_add_defaults(EXTRACTOR_OPTION_DEFAULT_POLICY);
#endif

	/*
	 * init processor mutex
	 */
	g_mutex_init(&tagsistant_processor_mutex);

	/*
	 * init some useful regex
	 */
	tagsistant_rx_date = g_regex_new(
		"^([0-9][0-9][0-9][0-9]):([0-9][0-9]):([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])$",
		0, 0, NULL);

	tagsistant_rx_cleaner = g_regex_new("[/ ]", 0, 0, NULL);

	/*
	 * quit if no autotagging was required at mount
	 */
	if (tagsistant.no_autotagging) {
		dbg('p', LOG_INFO, "Skipping plugin loading because -a was specified");
		if (!tagsistant.quiet) fprintf(stderr, " *** skipping plugin loading because -a was specified");
		return;
	}

	/*
	 * get the plugin dir from the environment variable
	 * TAGSISTANT_PLUGINS or from the default macro PLUGINS_DIR
	 */
	char *tagsistant_plugins = NULL;
	if (getenv("TAGSISTANT_PLUGINS") isNot NULL) {
		tagsistant_plugins = g_strdup(getenv("TAGSISTANT_PLUGINS"));
		if (!tagsistant.quiet) fprintf(stderr, " Using user defined plugin dir: %s\n", tagsistant_plugins);
	} else {
		tagsistant_plugins = g_strdup(PLUGINS_DIR);
		if (!tagsistant.quiet) fprintf(stderr, " Using default plugin dir: %s\n", tagsistant_plugins);
	}

	/*
	 * scan the filesystem for plugins
	 */
	struct stat st;
	if (lstat(tagsistant_plugins, &st) is -1) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** error opening directory %s: %s ***\n", tagsistant_plugins, strerror(errno));
	} else if (!S_ISDIR(st.st_mode)) {
		if (!tagsistant.quiet)
			fprintf(stderr, " *** error opening directory %s: not a directory ***\n", tagsistant_plugins);
	} else {
		/*
		 * open directory and read contents
		 */
		DIR *p = opendir(tagsistant_plugins);
		if (p is NULL) {
			if (!tagsistant.quiet)
				fprintf(stderr, " *** error opening plugin directory %s ***\n", tagsistant_plugins);
		} else {
			struct dirent *de = NULL;
			while ((de = readdir(p)) isNot NULL) {
				/* checking if file begins with tagsistant plugin prefix */
				char *needle = strstr(de->d_name, TAGSISTANT_PLUGIN_PREFIX);
				if ((needle is NULL) || (needle isNot de->d_name)) continue;

#				ifdef MACOSX
#					define PLUGIN_EXT ".dylib"
#				else
#					define PLUGIN_EXT ".so"
#				endif

				needle = strstr(de->d_name, PLUGIN_EXT);
				if ((needle is NULL) || (needle isNot de->d_name + strlen(de->d_name) - strlen(PLUGIN_EXT)))
					continue;

				/*
				 * file is a tagsistant plugin (beginning by right prefix) and
				 * is processed allocate new plugin object
				 */
				tagsistant_plugin_t *plugin = g_new0(tagsistant_plugin_t, 1);

				if (plugin is NULL) {
					dbg('p', LOG_ERR, "Error allocating plugin object");
					continue;
				}

				char *pname = g_strdup_printf("%s/%s", tagsistant_plugins, de->d_name);

				/*
				 * load the plugin
				 */
				plugin->handle = dlopen(pname, RTLD_NOW/* |RTLD_GLOBAL */);
				if (plugin->handle is NULL) {
					if (!tagsistant.quiet)
						fprintf(stderr, " *** error dlopen()ing plugin %s: %s ***\n", de->d_name, dlerror());
					g_free_null(plugin);
				} else {
					/*
					 * search for init function and call it
					 */
					int (*init_function)() = NULL;
					init_function = dlsym(plugin->handle, "tagsistant_plugin_init");
					if (init_function) {
						// TODO valgrind says: check for leaks
						int init_res = init_function();
						if (!init_res) {
							/*
							 * if init failed, ignore this plugin
							 */
							dbg('p', LOG_ERR, " *** error calling plugin_init() on %s ***\n", de->d_name);
							g_free_null(plugin);
							continue;
						}
					}

					/*
					 * search for MIME type string
					 */
					plugin->mime_type = dlsym(plugin->handle, "mime_type");
					if (plugin->mime_type is NULL) {
						if (!tagsistant.quiet)
							fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
						g_free_null(plugin);
					} else {
						/*
						 * search for processor function
						 */
						plugin->processor = dlsym(plugin->handle, "tagsistant_processor");
						if (plugin->processor is NULL) {
							if (!tagsistant.quiet)
								fprintf(stderr, " *** error finding %s processor function: %s ***\n", de->d_name, dlerror());
							g_free_null(plugin);
						} else {
							plugin->free = dlsym(plugin->handle, "tagsistant_plugin_free");
							if (plugin->free is NULL) {
								if (!tagsistant.quiet)
									fprintf(stderr, " *** error finding %s free function: %s (still registering the plugin) ***", de->d_name, dlerror());
							}

							/*
							 * add this plugin on queue head
							 */
							plugin->filename = g_strdup(de->d_name);
							plugin->next = tagsistant.plugins;
							tagsistant.plugins = plugin;
							if (!tagsistant.quiet)
								fprintf(stderr, " Loaded plugin: %20s -> %s\n", plugin->mime_type, plugin->filename);
						}
					}
				}
				g_free_null(pname);
			}
			closedir(p);
		}
	}

	g_free_null(tagsistant_plugins);
}

/**
 * Unloads all the plugins and disposes the regular expressions
 */
void tagsistant_plugin_unloader()
{
	/* unregistering plugins */
	tagsistant_plugin_t *pp = tagsistant.plugins;
	tagsistant_plugin_t *ppnext = pp;
	while (pp isNot NULL) {
		/* call plugin free method to let it free allocated resources */
		if (pp->free isNot NULL) {
			(pp->free)();
		}
		g_free_null(pp->filename);	/* free plugin filename */
		dlclose(pp->handle);		/* unload the plugin */
		ppnext = pp->next;			/* save next plugin in tagsistant chain */
		g_free_null(pp);			/* free this plugin entry in tagsistant chain */
		pp = ppnext;				/* point to next plugin in tagsistant chain */
	}

	g_regex_unref(tagsistant_rx_date);
}

/**
 * Apply a regular expression to a buffer (first N bytes of a file) and use each
 * matched token to tag the object
 *
 * @param qtree the querytree object to be tagged
 * @param buf the text to be matched by the regex
 * @param m a mutex to protect the regular expression
 * @param rx the regular expression
 */
void tagsistant_plugin_apply_regex(const tagsistant_querytree *qtree, const char *buf, GMutex *m, GRegex *rx)
{
	GMatchInfo *match_info;

	/* apply the regex, locking the mutex if provided */
	if (m isNot NULL) g_mutex_lock(m);
	g_regex_match(rx, buf, 0, &match_info);
	if (m isNot NULL) g_mutex_unlock(m);

	/* process the matched entries */
	while (g_match_info_matches(match_info)) {
		gchar *raw = g_match_info_fetch(match_info, 1);
		dbg('p', LOG_INFO, "Found raw data: %s", raw);

		gchar **tokens = g_strsplit_set(raw, " \t,.!?/", 255);
		g_free_null(raw);

		int x = 0;
		while (tokens[x]) {
			if (strlen(tokens[x]) >= 3) tagsistant_sql_tag_object(qtree->dbi, tokens[x], NULL, NULL, qtree->inode);
			x++;
		}

		g_strfreev(tokens);
		g_match_info_next(match_info, NULL);
	}
}

