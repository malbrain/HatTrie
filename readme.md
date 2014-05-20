The HatTrie code is compiled with this command line:

cl HatTrie64d.c /Ox -- or -- cc HatTrie64d.c -o HatTrie64d

HatTrie64d [load file name] [search file name] [# root levels] [# of slots in a pail node] [hash bucket slots] [maximum strings per hash bucket] [smallest string array size in 16 byte units] ...

All parameters after the two file names are optional, but positional, and are supplied with the following default values:

  * root levels = 3      -- three radix levels booted into the tree
  * pail slots = 127     -- number of slots for array nodes in a pail, zero to disable pails
  * bucket slots = 2047  -- number of slots for array nodes or pail nodes in bucket node
  * bucket max = 65536   -- number of strings in all contained string arrays
  * smallest array = 1   -- in units of 16 bytes
  * next larger sizes = 2 3 4 6 8 10 12 14 16 24 32 

Up to 28 array sizes can be specified.  Two dataset files, distinct_1 for loading the trie and skew1_1 for searching it, are available at Dr. Nikolas Askitis' web site: http://www.naskitis.com

Supplying an empty search file name will cause the sorted load file to be written to std-out.  Compiling with -D REVERSE will cause the reverse sorted order to be written.

Sample invocation of loading distinct_1 and searching skew1_1:

HATtrie64c distinct_1 skew1_1 3 127 2047 65536 1 2 3 4 6 8 12 16 24 32 48 64 96 128 256

Sample invocation as string sorter:

HATtrie64 distinct_1 "" 3 > tst.out

