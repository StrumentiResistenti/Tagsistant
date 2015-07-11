Tagsistant is a semantic file system for Linux, a personal tool
to catalog files using tags (labels, mnemonic informations)
rather than directories.

Tagsistant replace the concept of directory with that of tag, but
since it have to do with directories, it pairs the concept of tag
with that of directory. So within Tagsistant a tag is a directory
and a directory is a tag.

To be more precise, this is not true with all the directories.
First level directories are special. The one called `tags/` hosts
all the tags. This means that every directory created inside it
is infact a tag.

Another, called `store/`, hosts contents, like files. All the tags
created inside `tags/` are available inside `store/`. To tag a file
all you have to do is to copy it inside one or more directories
under `store/`.

Another special first level directory is `relations/`. Inside it
you can establish relations between tags using mkdir:

```bash
 $ mkdir relations/music/includes/rock
 $ mkdir relations/rock/includes/beatles
 $ mkdir relations/beatles/includes/lennon
 $ mkdir relations/beatles/is_equivalent/the_beatles
 $ mkdir relations/lennon/requires/beatles
```

A reasoner follows the relations you establish to include objects
as a result of your queries. This is an example:

```bash
 $ cp ~/let_it_be.mp3 store/lennon/@/
 $ ls store/the_beatles/@/
 let_it_be.mp3
```

The file `let_it_be.mp3` is tagged as `lennon`, which is included by
`beatles`, which is equivalent to `the_beatles`, and so is listed.

Tagsistant also comes with a plugin API to extend its behaviour. Some
plugins for `.ogg`, `.mp3`, `.xml`, `.html` and other formats are provided.
What they do is add more tags to a file, using specific procedures.

And if a file is copied inside Tagsistant twice, Tagsistant is able
to pair the second copy with the first one, keeping it just one.

More information is available at: http://www.tagsistant.net/howto

To compile Tagsistant, clone this repo and do:

```bash
$ ./autogen.sh
$ ./configure
$ make
```

To install Tagsistant, do:

```bash
$ make install
```
