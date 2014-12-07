/*
   Tagsistant (tagfs) -- path_resolution.h
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

/**
 * Triple tag operators
 */
enum {
	TAGSISTANT_NONE,
	TAGSISTANT_EQUAL_TO,
	TAGSISTANT_CONTAINS,
	TAGSISTANT_GREATER_THAN,
	TAGSISTANT_SMALLER_THAN,
	TAGSISTANT_UNDEFINED_OPERATOR
} tagsistant_query_operators;

/**
 * Triple tag operators strings
 */
#define TAGSISTANT_EQUALS_TO_OPERATOR    "eq"
#define TAGSISTANT_CONTAINS_OPERATOR     "inc"
#define TAGSISTANT_GREATER_THAN_OPERATOR "gt"
#define TAGSISTANT_SMALLER_THAN_OPERATOR "lt"

/**
 * the regex used to check relations in the relations/ queries
 */
#define TAGSISTANT_RELATION_PATTERN "^includes|excludes|is_equivalent|requires$"

/**
 * guess if a relation is admitted or not
 */
#define IS_VALID_RELATION(relation) g_regex_match_simple(TAGSISTANT_RELATION_PATTERN, relation, G_REGEX_EXTENDED, 0)

/**
 * defines a token in a query path
 */
typedef struct qtree_and_node {
	/** this tag should not match? */
	int negate;

	/** the name of this token */
	char *tag;
	tagsistant_tag_id tag_id;

	/** the namespace **/
	char *namespace;

	/** the key **/
	char *key;

	/** the operator **/
	int operator;

	/** the value **/
	char *value;

	/** list of all related tags **/
	struct qtree_and_node *related;

	/** list of all negated tags **/
	struct qtree_and_node *negated;

	/** next AND token */
	struct qtree_and_node *next;
} qtree_and_node;

/**
 * define an OR section in a query path
 */
typedef struct qtree_or_node {
	/** the next OR section */
	struct qtree_or_node *next;

	/** the list of AND tokens */
	struct qtree_and_node *and_set;
} qtree_or_node;

/*
 * depeding on relative path, a query can be one in the following:
 */
typedef enum {
	QTYPE_MALFORMED,	// wrong path (not starting by /tags, /archive, /stats or /relations)
	QTYPE_ROOT,			// no path, that's a special case for root directory
	QTYPE_ARCHIVE,		// path pointing to objects on disk, begins with /archive/
	QTYPE_TAGS,			// path that's a query, begins with /tags/
	QTYPE_RETAG,		// experimental path used for object retagging
	QTYPE_RELATIONS,	// path that's a relation between two or more tags, begins with /relations/
	QTYPE_STATS,		// path that's a special query for internal status, begins with /stats/
	QTYPE_STORE,		// where the files are tagged and accessed
	QTYPE_ALIAS,		// where query aliases (bookmarks) are kept
	QTYPE_TOTAL
} tagsistant_query_type;

/**
 * An array of human readable definitions of each query type
 */
extern gchar *tagsistant_querytree_types[QTYPE_TOTAL];

/**
 * returns the type of query described by a tagsistant_querytree struct
 * WARNING: the returned string MUST NOT BE FREED!
 */
#define tagsistant_querytree_type(qtree) tagsistant_querytree_types[qtree->type]

/*
 * to ease coding, there are some macros to check
 * if a query if of a given type
 */
#define QTREE_IS_MALFORMED(qtree)	(QTYPE_MALFORMED == qtree->type)
#define QTREE_IS_ROOT(qtree)		(QTYPE_ROOT == qtree->type)
#define QTREE_IS_TAGS(qtree)		(QTYPE_TAGS == qtree->type)
#define QTREE_IS_ARCHIVE(qtree)		(QTYPE_ARCHIVE == qtree->type)
#define QTREE_IS_RELATIONS(qtree)	(QTYPE_RELATIONS == qtree->type)
#define QTREE_IS_STATS(qtree)		(QTYPE_STATS == qtree->type)
#define QTREE_IS_RETAG(qtree)		(QTYPE_RETAG == qtree->type)
#define QTREE_IS_STORE(qtree)		(QTYPE_STORE == qtree->type)
#define QTREE_IS_ALIAS(qtree)		(QTYPE_ALIAS == qtree->type)

/*
 * if a query points to an object on disk this returns true;
 * that's:
 *
 *   archive/something
 *   tags/t1/t2/.../tN/=/something
 */
#define QTREE_POINTS_TO_OBJECT(qtree) (qtree->points_to_object)

/*
 * some more info about a query:
 * is_taggable -> points_to_object but on first level (so not on tags/t1/t2/.../tN/=/something/more/...)
 * is_complete -> query is of type tags/ and has an =/
 * is_external -> the query points outside tagsistant mountpoint
 * is_internal -> the query points inside tagsistant mountpoint
 */
#define QTREE_IS_TAGGABLE(qtree) (qtree->is_taggable)
#define QTREE_IS_COMPLETE(qtree) (qtree->complete)
#define QTREE_IS_EXTERNAL(qtree) (qtree->is_external)
#define QTREE_IS_INTERNAL(qtree) (!qtree->is_external)

/*
 * two queries are of the same type and are both complete
 * the second is true for tags/ if both are complete,
 * and always for other types of queries
 */
#define QTREES_ARE_SIMILAR(qtree1, qtree2) ((qtree1->type == qtree2->type) && (qtree1->complete == qtree2->complete))

/*
 * check if a path is external to tagsistant mountpoint
 * without requiring query resolution and querytree building
 */
#define TAGSISTANT_PATH_IS_EXTERNAL(path) (g_strstr_len(path, strlen(path), tagsistant.mountpoint) != path)

/**
 * define the querytree structure
 * that holds a tree of qtree_or_node_t
 * and qtree_and_node_t and a string
 * containing the file part of the path.
 */
typedef struct querytree {
	/** the complete path that generated the tree */
	/** i.e. <MPOINT>/tags/t1/+/t2/=/object/path.txt */
	gchar *full_path;

	/** the complete path after the alias expansion */
	gchar *expanded_full_path;

	/** the path of the object, if provided */
	/** i.e. object/path.txt */
	gchar *object_path;

	/** the path of the object on disk */
	/** NNN___object/path.txt */
	gchar *archive_path;

	/** like the previous one, but with current archive path prefixed */
	/** ~/.tagsistant/archive/NNN___object/path.txt */
	gchar *full_archive_path;

	/** the inode of the object, if directly managed by tagsistant */
	tagsistant_inode inode;

	/** which kind of path is this? see tagsistant_query_type */
	int type;

	/**
	 * the query points to an object on disk?
	 * it's true if it's an archive/ query or a complete store/ query
	 */
	int points_to_object;

	/** the object path pointed to is taggable? (one element path) */
	int is_taggable;

	/** the object is external to tagsistant mountpoint */
	int is_external;

	/** last tag found while parsing a /tags query */
	gchar *last_tag;

	/** the query is valid */
	int valid;

	/** the tags/ query is complete? it has a '=' sign? */
	int complete;

	/** the object pointed by is currently in the database? */
	int exists;

	/**
	 * force the use of inodes in filenames even when filenames are not ambiguous
	 * useful on queries with triple tags and operators different from eq/
	 */
	int force_inode_in_filenames;

	/** the query tree in a tags/ query */
	qtree_or_node *tree;

	/** used during path parsing to specify that next tag should not match */
	int negate_next_tag;

	/** the first tag in a relations/ query */
	gchar *first_tag;

	/** the second tag in a relations/ query */
	gchar *second_tag;

	/** the triple tag namespace **/
	gchar *namespace;

	/** the triple tag key **/
	gchar *key;

	/** the triple tag operator **/
	int    operator;

	/** the triple tag value **/
	gchar *value;

	/** the related riple tag namespace */
	gchar *related_namespace;

	/** the related triple tag key */
	gchar *related_key;

	/** the related triple tag operator */
	int    related_operator;

	/** the related triple tag value */
	gchar *related_value;

	/** the relation in a relations/ query */
	gchar *relation;

	/** the path in a stats/ query */
	gchar *stats_path;

	/** the alias in the alias/ folder */
	gchar *alias;

	/** libDBI connection handle */
	dbi_conn dbi;

	/** record if a transaction has been opened on this connection */
	int transaction_started;

	/** last time the cached copy of this querytree has been accessed */
	GTimeSpan last_access_microsecond;

	/** do reasoning or not? */
	int do_reasoning;

	/**
	 * if true, the object pointed will be deleted from the archive
	 * directory by tagsistant_querytree_destroy()
	 */
	int schedule_for_unlink;

	/**
	 * if the query is wrong, this field will hold am error message
	 */
	gchar *error_message;

} tagsistant_querytree;

/**
 * used in linked list of returned results
 *
 * Alessandro AkiRoss Re reported a conflict with the structure
 * file_handle in /usr/include/bits/fcntl.h on Fedora 15
 */
typedef struct {
	char name[1024];			/** object filename */
	tagsistant_inode inode;		/** object inode */
} tagsistant_file_handle;

/**
 * reasoning structure to trace reasoning process
 */
typedef struct {
	qtree_and_node *start_node;
	qtree_and_node *current_node;
	int added_tags;
	dbi_conn conn;
	int negate;
} tagsistant_reasoning;

#if 0
/**
 * applies a function to all the qtree_and_node_t nodes of
 * a tagstistant_querytree_t structure. the function applied must be
 * declared as:
 *
 *   void function(dbi_conn dbi, qtree_and_node *node, ...);
 *
 * @param qtree the tagsistant_querytree structure to traverse
 * @param funcpointer the function pointer
 */
#define old_tagsistant_querytree_traverse(qtree, funcpointer, ...) {\
	if (NULL != qtree) {\
		qtree_or_node *ptx = qtree->tree;\
		while (NULL != ptx) {\
			qtree_and_node *andptx = ptx->and_set;\
			while (NULL != andptx) {\
				if (andptx->tag) {\
					funcpointer(qtree->dbi, andptx->tag, NULL, NULL, ##__VA_ARGS__);\
				} else {\
					funcpointer(qtree->dbi, andptx->namespace, andptx->key, andptx->value, ##__VA_ARGS__);\
				}\
				andptx = andptx->next;\
			}\
			ptx = ptx->next;\
		}\
	}\
}
#endif

/**
 * a pointer to a variadic function that is applied to each node in
 * a querytree and_set tree
 *
 * @param dbi a DBI connection
 * @param namespace a namespace or tag
 * @param a key, if the second parameter is a namespace
 * @param a value, if the second parameter is a namespace
 */
typedef void (*tagsistant_querytree_traverser)(
	dbi_conn dbi,
	const gchar *namespace,
	const gchar *key,
	const gchar *value,
	tagsistant_inode opt_inode);

/**
 * an utility function which iterates over each node of a
 * querytree and_set tree to apply the function pointed by
 * funcpointer
 *
 * @param qtree the querytree object to descend
 * @param funcpointer the function to be applied to each tree node
 */
extern void tagsistant_querytree_traverse(
	tagsistant_querytree *qtree,
	tagsistant_querytree_traverser funcpointer,
	tagsistant_inode opt_inode);

// querytree functions
extern void						tagsistant_path_resolution_init();
extern void						tagsistant_reasoner_init();

extern tagsistant_querytree *	tagsistant_querytree_new(const char *path, int assign_inode, int start_transaction, int provide_connection, int disable_reasoner);
extern void 					tagsistant_querytree_destroy(tagsistant_querytree *qtree, guint commit_transaction);

extern void						tagsistant_querytree_set_object_path(tagsistant_querytree *qtree, char *new_object_path);
extern void						tagsistant_querytree_set_inode(tagsistant_querytree *qtree, tagsistant_inode inode);
extern tagsistant_query_type	tagsistant_querytree_guess_type(gchar **token_ptr);
extern int						tagsistant_querytree_check_tagging_consistency(tagsistant_querytree *qtree);

extern int						tagsistant_querytree_deduplicate(tagsistant_querytree *qtree);
extern int						tagsistant_querytree_cache_total();

// caching functions
extern void						tagsistant_invalidate_querytree_cache(tagsistant_querytree *qtree);
extern void						tagsistant_invalidate_and_set_cache_entries(tagsistant_querytree *qtree);

// inode functions
extern tagsistant_inode			tagsistant_inode_extract_from_path(const gchar *path);
extern tagsistant_inode			tagsistant_inode_extract_from_querytree(tagsistant_querytree *qtree);
extern gchar *					tagsistant_get_reversed_inode_tree(tagsistant_inode inode);

// reasoner functions
#define 						tagsistant_reasoner(reasoning) tagsistant_reasoner_inner(reasoning, 1)
extern int						tagsistant_reasoner_inner(tagsistant_reasoning *reasoning, int do_caching);
extern void						tagsistant_invalidate_reasoning_cache(gchar *tag);

/**
 * ERROR MESSAGES
 **/
#define TAGSISTANT_ERROR_MALFORMED_QUERY \
	"Syntax error: your query is malformed\n"

#define TAGSISTANT_ERROR_NULL_QUERY \
	"Syntax error: null query. Specify at least one tag between store/ and @/ or @@/."

#define TAGSISTANT_ERROR_NESTED_TAG_GROUP \
	"Syntax error: nested tag group. Close all tag groups before opening another.\n"

#define TAGSISTANT_ERROR_CLOSE_TAG_GROUP_NOT_OPENED \
	"Syntax error: } without {. Open a tag group before closing it\n"

#define TAGSISTANT_ERROR_DOUBLE_NEGATION \
	"Syntax error: can't do a double negation. Use the -/ operator before a tag and never write -/-/\n"

#define TAGSISTANT_ERROR_NEGATION_INSIDE_TAG_GROUP \
	"Syntax error: negation inside a tag group is prohibited\n"

#define TAGSISTANT_ERROR_MEMORY_ALLOCATION \
	"Internal error: can't allocate enough memory\n"

#define TAGSISTANT_ERROR_NEGATION_ON_FIRST_POSITION \
	"Syntax error: negation can't start a query or follow a +/ operator"
