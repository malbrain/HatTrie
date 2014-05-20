
//	Author Karl Malbrain, malbrain@cal.berkeley.edu

//	Implement Simplified HAT-trie w/associated data areas,
//	and bi-directional cursors

//	Adapted from the ideas of Douglas Baskins of HP
//	and Dr. Askitis.

//	The ASKITIS benchmarking option was implemented with 
//	assistance from Dr. Nikolas Askitis (www.naskitis.com). 

//	functions:
//	hat_open:	open a new hat array returning a hat object.
//	hat_close:	close an open hat array, freeing all memory.
//	hat_data:	allocate data memory within hat array for external use.
//	hat_cell:	insert a string into the HAT tree, return associated data addr.
//	hat_cursor:	return a sort cursor for the HAT tree. Free with free().
//	hat_key:	return the key from the HAT trie at the current cursor location.
//	hat_nxt:	move the cursor to the next key in the HAT trie, return TRUE/FALSE.
//	hat_prv:	move the cursor to the prev key in the HAT trie, return TRUE/FALSE.
//	hat_start:	move the cursor to the first key >= given key, return TRUE/FALSE.
//	hat_last:	move the cursor to the last key in the HAT trie, return TRUE/FALSE
//	hat_slot:	return the pointer to the associated data area for cursor.

#ifdef linux
	#define _GNU_SOURCE
	#define _FILE_OFFSET_BITS 64
	#define _LARGEFILE_SOURCE
	#define _LARGEFILE64_SOURCE
	#define __USE_FILE_OFFSET64

	#include <endian.h>
#else
	#ifdef __BIG_ENDIAN__
		#ifndef BYTE_ORDER
			#define BYTE_ORDER 4321
		#endif
	#else
		#ifndef BYTE_ORDER
			#define BYTE_ORDER 1234
		#endif
	#endif
	#ifndef BIG_ENDIAN
		#define BIG_ENDIAN 4321
	#endif
#endif

#include <stdlib.h>
#include <memory.h>
#include <string.h>

#if defined(_WIN32)
typedef unsigned short ushort;
#endif

typedef unsigned char uchar;
typedef unsigned int uint;
#define PRIuint			"u"

#if defined(__LP64__) || \
	defined(__x86_64__) || \
	defined(__amd64__) || \
	defined(_WIN64) || \
	defined(__sparc64__) || \
	defined(__arch64__) || \
	defined(__powerpc64__) || \
	defined (__s390x__) 
	//	defines for 64 bit
	
	typedef unsigned long long HatSlot;
	#define HAT_slot_size 8

	#define PRIhatvalue	"llu"

#else
	//	defines for 32 bit
	
	typedef uint HatSlot;
	#define HAT_slot_size 4

	#define PRIhatvalue	"u"

#endif

#define HAT_mask (~(HatSlot)0x07)
#define HAT_type ((HatSlot)0x07)

#define HAT_node_size	16

typedef struct {
	HatSlot array[0];	// hash array of pail arrays
} HatPail;

typedef struct {
	uint count;
	HatSlot slots[0];
} HatBucket;

#define HAT_cache_line 8	// allocation granularity is 8 bytes

#include <assert.h>
#include <stdio.h>

unsigned long long MaxMem = 0;
unsigned long long Searches = 0;
unsigned long long Probes = 0;
unsigned long long Bucket = 0;
unsigned long long Pail = 0;
unsigned long long Radix = 0;
unsigned long long Small = 0;

// void hat_abort (char *msg) __attribute__ ((noreturn)); // Tell static analyser that this function will not return
void hat_abort (char *msg)
{
	fprintf(stderr, "%s\n", msg);
	exit(1);
}

//	allow room for 64K bucket slots and HatSeg structure

#define HAT_seg	(65536 * HAT_slot_size + 32)

enum HAT_types {
	HAT_radix		= 0,	// radix nodes
	HAT_bucket		= 1,	// bucket nodes
	HAT_array		= 2,	// linear array nodes
	HAT_pail		= 3,	// hashed linear array nodes
	HAT_1			= 4,
	HAT_2			= 5,
	HAT_3			= 6,
	HAT_4			= 7,
	HAT_6			= 8,
	HAT_8			= 9,
	HAT_10			= 10,
	HAT_12			= 11,
	HAT_14			= 12,
	HAT_16			= 13,
	HAT_24			= 14,
	HAT_32			= 15,
};

uint HatSize[32] = {
	(HAT_slot_size * 128),	// HAT_radix node size
	(sizeof(HatBucket)),	// HAT_bucket node size
	(0),					// HAT_array node size below
	(sizeof(HatPail)),		// HAT_pail node size
	(1 * HAT_node_size),	// HAT_1 array size
	(2 * HAT_node_size),	// HAT_2 array size
	(3 * HAT_node_size),	// HAT_3 array size
	(4 * HAT_node_size),	// HAT_4 array size
	(6 * HAT_node_size),	// HAT_6 array size
	(8 * HAT_node_size),	// HAT_8 array size
	(10 * HAT_node_size),	// HAT_10 array size
	(12 * HAT_node_size),	// HAT_12 array size
	(14 * HAT_node_size),	// HAT_14 array size
	(16 * HAT_node_size),	// HAT_16 array size
	(24 * HAT_node_size),	// HAT_24 array size
	(32 * HAT_node_size),	// HAT_32 array size
};

uint HatBucketSlots = 2047;
uint HatBucketMax = 65536;
uint HatPailMax = 127;

uchar HatMax = HAT_32;

typedef struct {
	void *seg;			// next used allocator
	uint next;			// next available offset
} HatSeg;

typedef struct {
	void **reuse[32];	// reuse hat blocks
	int counts[32];		// hat block counters
	HatSeg *seg;		// current hat allocator
	uint bootlvl;		// cascaded radix nodes in root
	uint aux;			// auxilliary bytes per key
	HatSlot root[0];	// base root of hat array
} Hat;

typedef struct {
	ushort nxt;			// next key array allocation
	uchar type;			// type of base node
	uchar cnt;			// next data area allocation
	uchar keys[0];		// keys byte array
} HatBase;

typedef struct {
	uchar *key;			// pointer to key string
	void *slot;			// user data area
} HatSort;

typedef struct {
	int cnt;			// number of bucket keys
	int idx;			// current bucket index
	short top;			// current stack top
	ushort aux;			// number of aux bytes per key
	int rootlvl;		// number of root levels
	uint maxroot;		// count of root array slots
	uint rootscan;		// triple root scan index
	HatSlot next[256];	// radix node stack
	uchar scan[256];	// radix node scan index stack
	HatSort keys[0];	// sorted array for bucket
} HatCursor;

int hat_nxt (HatCursor *cursor);

//	ternery quick sort of cursor's keys
//	modelled after R Sedgewick's
//	"Quicksort with 3-way partitioning"

vecswap (int i, int j, int n, HatSort *x)
{
HatSort swap[1];

	while( n-- ) {
		*swap = x[i];
		x[i++] = x[j];
		x[j++] = *swap;
	}	
}

void hat_qsort (HatSort *x, int n, uchar o)
{
ushort skip, skipb, skipc, len;
uchar pivot, chb, chc, *key;
int a, b, c, d, r;
HatSort swap[1];

  while( n > 10 ) {
	a = rand () % n;

	*swap = x[0];
	x[0] = x[a];
	x[a] = *swap;

	len = x[0].key[0];

	if( len & 0x80 )
		len &= 0x7f, len += x[0].key[1] << 7, skip = 2;
	else
		skip = 1;

	if( len > o )
		pivot = x[0].key[o+skip];
	else
		pivot = 0;

	a = b = 1;
	c = d = n - 1;

	while( 1 ) {
		while( b <= c ) {
		  len = x[b].key[0];

		  if( len & 0x80 )
			len &= 0x7f, len += x[b].key[1] << 7, skip = 2;
		  else
			skip = 1;

		  if( len > o )
			chb = x[b].key[o+skip];
		  else
			chb = 0;
		  if( chb > pivot )
			break;
		  if( chb == pivot ) {
			*swap = x[a];
			x[a++] = x[b];
			x[b] = *swap;
		  }
		  b += 1;
		}

		while( b <= c ) {
		  len = x[c].key[0];

		  if( len & 0x80 )
			len &= 0x7f, len += x[c].key[1] << 7, skip = 2;
		  else
			skip = 1;

		  if( len > o )
			chc = x[c].key[o+skip];
		  else
			chc = 0;
		  if( chc < pivot )
			break;
		  if( chc == pivot ) {
			*swap = x[c];
			x[c] = x[d];
			x[d--] = *swap;
		  }
		  c -= 1;
		}

		if( b > c )
			break;

		*swap = x[b];
		x[b++] = x[c];
		x[c--] = *swap;
	}

	r = a < b-a ? a : b-a;
	vecswap (0, b-r, r, x);

	r = d-c < n-d-1 ? d-c : n-d-1;
	vecswap (b, n-r, r, x);

	if( r = d - c )
		hat_qsort (x + n - r, r, o);

	if( r = b - a )
		hat_qsort (x, r, o);

	len = x[r].key[0];

	if( len & 0x80 )
		len &= 0x7f, len += x[r].key[1] << 7;

	if( len == o )
		return;

	n += a - d - 1;
	x += r;
	o += 1;
  }

  if( n > 1 ) {
	a = 0;

	while( ++a < n )
	  for( b = a; b > 0; b-- ) {
		chb = x[b-1].key[0];
		if( chb & 0x80 )
			chb &= 0x7f, chb += x[b-1].key[1] << 7, skipb = 2;
		else
			skipb = 1;
		chc = x[b].key[0];
		if( chc & 0x80 )
			chc &= 0x7f, chc += x[b].key[1] << 7, skipc = 2;
		else
			skipc = 1;
		r = o;
		d = 0;

		while( r < chb && r < chc )
		  if( d = x[b-1].key[r+skipb] - x[b].key[r+skipc] )
			break;
		  else
			r++;

		if( d > 0 || d == 0 && chb > chc ) {
		  *swap = x[b];
		  x[b] = x[b-1];
		  x[b-1] = *swap;
		}
	  }
  }
}

//	strip out pointers from HAT_array node
//	to elements of the sorted array

int hat_strip_array (HatCursor *cursor, HatSlot node, HatSort *list)
{
HatBase *base = (HatBase *)(node & HAT_mask);
uint size = HatSize[base->type];
ushort tst = 0;
ushort cnt = 0;
ushort len;

  while( tst < base->nxt ) {
	list[cnt].slot = (uchar *)base + size - (cnt+1) * cursor->aux;
	list[cnt].key = base->keys + tst;
	len = base->keys[tst++];
	if( len & 0x80 )
		len &= 0x7f, len += base->keys[tst++] << 7;
	tst += len;
	cnt++;
  }

  return cnt;
}

int hat_strip_pail (HatCursor *cursor, HatSlot node, HatSort *list)
{
HatPail *pail = (HatPail *)(node & HAT_mask);
uint total = 0;
int idx;

	for( idx = 0; idx < HatPailMax; idx++ )
	  if( pail->array[idx] )
		total += hat_strip_array (cursor, pail->array[idx], list);

	return total;
}

//	sort current bucket into cursor array

//	find and sort current node entry
//  either Bucket or Array

void hat_sort (HatCursor *cursor)
{
HatBucket *bucket;
uint off, idx;
uchar len, ch;
uint cnt;

  switch( cursor->next[cursor->top] & HAT_type ) {
  case HAT_array:
	cursor->cnt = hat_strip_array (cursor, cursor->next[cursor->top], cursor->keys);
	break;

  case HAT_pail:
	cursor->cnt = hat_strip_pail (cursor, cursor->next[cursor->top], cursor->keys);
	break;

  case HAT_bucket:
	bucket = (HatBucket *)(cursor->next[cursor->top] & HAT_mask);
	cursor->cnt = 0;

	for( idx = 0; idx < HatBucketSlots; idx++ )
	  switch( bucket->slots[idx] & HAT_type ) {
	  case HAT_array:
		cursor->cnt += hat_strip_array (cursor, bucket->slots[idx], cursor->keys + cursor->cnt);
		continue;
	  case HAT_pail:
		cursor->cnt += hat_strip_pail (cursor, bucket->slots[idx], cursor->keys + cursor->cnt);
		continue;
	  }

	break;

  }

  hat_qsort (cursor->keys, cursor->cnt, 0);
}

int hat_greater (HatCursor *cursor, uchar *buff, uint max)
{
ushort len;
ushort tst;

  //	find first key >= given key

  for( cursor->idx = 0; cursor->idx < cursor->cnt; cursor->idx++ ) {
	tst = 1;
    len = cursor->keys[cursor->idx].key[0];
	if( len & 0x80 )
		len &= 0x7f, len += cursor->keys[cursor->idx].key[1] << 7, tst = 2;
    if( memcmp (cursor->keys[cursor->idx].key + tst, buff, len > max ? max : len) )
  		continue;
    if( len >= max )
  		return 1;
  }

  //	given key > every key in bucket

  return hat_nxt (cursor);
}

//	open new sort cursor into collection

void *hat_cursor (Hat *hat)
{
HatCursor *cursor;
uint size;

	size = sizeof(HatCursor) + HatBucketMax * sizeof(HatSort);
	cursor = malloc (size);
	memset (cursor, 0, size);

	cursor->next[0] = (HatSlot)hat->root;
	cursor->aux = hat->aux;
	cursor->maxroot = 1;

	for( cursor->rootlvl = 0; cursor->rootlvl < hat->bootlvl; cursor->rootlvl++ )
		cursor->maxroot *= 128;

	return cursor;
}

void *hat_start (HatCursor *cursor, uchar *buff, uint max)
{
HatSlot *radix, *root;
HatSlot next;
uint off = 0;
uint idx;
uchar ch;

	if( max > 255 )
		max = 255;

	for( idx = 0; idx < cursor->rootlvl; idx++ ) {
		cursor->rootscan *= 128;
		if( off < max )
			cursor->rootscan += buff[off++];
	}

	//	find first root >= given key

	root = (HatSlot *)(cursor->next[0]);
	cursor->top = 0;

	if( next = root[cursor->rootscan] ) {
	  cursor->next[++cursor->top] = next;

loop:
	  if( (cursor->next[cursor->top] & HAT_type) == HAT_radix ) {
		if( max > off )
			ch = buff[off++];
		else
			ch = 0;

		radix = (HatSlot *)(cursor->next[cursor->top] & HAT_mask);

		while( ch < 128 )
		  if( radix[ch] ) {
			cursor->scan[cursor->top] = ch;
		    cursor->next[++cursor->top] = radix[ch];
			goto loop;
		  } else
			max = 0, ch++;

		//	given key > every key

		if( hat_nxt (cursor) )
			return cursor;

		free (cursor);
		return NULL;
	  }

	  hat_sort (cursor);
  	  cursor->idx = 0;

	  if( hat_greater (cursor, buff + off, max - off) )
	 	return cursor;

	  free (cursor);
	  return NULL;
	}

	//	scan to next occupied root

	cursor->top++;

	if( hat_nxt (cursor) )
		return cursor;

	free (cursor);
	return NULL;
}

//	return user area slot address at given cursor location

void *hat_slot (HatCursor *cursor)
{
	return cursor->keys[cursor->idx].slot;
}

//	advance cursor to next key
//	returning false if EOT

int hat_nxt (HatCursor *cursor)
{
HatSlot *radix;
uint idx, max;
uchar ch;

	//  any keys left in current sorted array?

	if( ++cursor->idx < cursor->cnt )
		return 1;

	//  move thru radix nodes
	//	slot zero is the triple root

  while( --cursor->top >= 0 ) {
	radix = (HatSlot *)(cursor->next[cursor->top] & HAT_mask);

	if( !cursor->top )
		max = cursor->maxroot;
	else
		max = 128;

	if( cursor->top )
		idx = cursor->scan[cursor->top];
	else
		idx = cursor->rootscan;

	while( ++idx < max )
	  if( radix[idx] ) {
		if( cursor->top )
			cursor->scan[cursor->top] = idx;
		else
			cursor->rootscan = idx;

		cursor->next[++cursor->top] = radix[idx];
loop:
		if( (cursor->next[cursor->top] & HAT_type) == HAT_radix ) {
		  radix = (HatSlot *)(cursor->next[cursor->top] & HAT_mask);

		  for( ch = 0; ch < 128; ch++ )
		   if( radix[ch] ) {
			cursor->scan[cursor->top] = ch;
		    cursor->next[++cursor->top] = radix[ch];
			goto loop;
		   }
		}

		hat_sort (cursor);
  	    cursor->idx = 0;
		return 1;
	  }
  }

  return 0;
}

//	advance cursor to previous key
//	returning false if BOI

int hat_prv (HatCursor *cursor)
{
HatSlot *radix;
uint idx, max;
uchar ch;

	//  any keys left in current sorted array?

	if( cursor->idx )
		return cursor->idx--, 1;

	//  move down thru radix nodes
	//	slot zero is the triple root

  while( --cursor->top >= 0 ) {
	radix = (HatSlot *)(cursor->next[cursor->top] & HAT_mask);

	if( cursor->top )
		idx = cursor->scan[cursor->top];
	else
		idx = cursor->rootscan;

	while( idx-- )
	  if( radix[idx] ) {
		if( cursor->top )
			cursor->scan[cursor->top] = idx;
		else
			cursor->rootscan = idx;

		cursor->next[++cursor->top] = radix[idx];
loop:
		if( (cursor->next[cursor->top] & HAT_type) == HAT_radix ) {
		  radix = (HatSlot *)(cursor->next[cursor->top] & HAT_mask);

		  for( ch = 128; ch-- > 0; )
		   if( radix[ch] ) {
			cursor->scan[cursor->top] = ch;
		    cursor->next[++cursor->top] = radix[ch];
			goto loop;
		   }
		}

		hat_sort (cursor);
		cursor->idx = cursor->cnt - 1;
		return 1;
	  }
  }

  return 0;
}

//	advance cursor to last key in the trie
//	returning false if tree is empty

int hat_last (HatCursor *cursor)
{
HatSlot *radix, next, *root;
uint idx, max;
uchar ch;

	//	find last root
	//	or return if tree is empty

	cursor->rootscan = cursor->maxroot;
	root = (HatSlot *)(cursor->next[0]);
	cursor->top = 0;

	while( cursor->rootscan )
	 if( next = root[--cursor->rootscan] )
	  break;
	 else if( !cursor->rootscan )
	  return 0;
	  
	cursor->next[++cursor->top] = next;

loop:
	if( (cursor->next[cursor->top] & HAT_type) == HAT_radix ) {
		radix = (HatSlot *)(cursor->next[cursor->top] & HAT_mask);
		ch = 128;

		while( ch-- )
		  if( radix[ch] ) {
			cursor->scan[cursor->top] = ch;
		    cursor->next[++cursor->top] = radix[ch];
			goto loop;
		  }
	}

	hat_sort (cursor);
  	cursor->idx = cursor->cnt - 1;
	return 1;
}

//	return key at current cursor location

uint hat_key (HatCursor *cursor, uchar *buff, uint max)
{
int idx, scan, len;
uchar *key, ch;
uint off = 0;

	max--;	// leave room for terminator

	//	is cursor at EOF?

	if( cursor->top < 0 ) {
	  if( max )
		buff[0] = 0;
	  return 0;
	}

	//	fill in from triple root
	//	and cascaded radix nodes

	for( idx = 0; idx < cursor->top; idx++ )
	  if( !idx ) {
		for( scan = cursor->rootlvl; scan--; )
		  if( ch = (cursor->rootscan >> scan * 7) & 0x7F )
	        if( off < max )
		      buff[off++] = ch;
	  } else if( off < max )
		  if( ch = cursor->scan[idx] ) // skip slot zero
			buff[off++] = ch;

	//	pull rest of key from current entry in sorted array

	key = cursor->keys[cursor->idx].key;
	len = *key++;

	if( len & 0x80 )
		len &= 0x7f, len += *key++ << 7;

	while( len-- && off < max )
		buff[off++] = *key++;

	buff[off] = 0;
	return off;
}

//	allocate hat node

void *hat_alloc (Hat *hat, uint type)
{
uint amt, idx, round;
HatSeg *seg;
void *block;

	amt = HatSize[type];
	hat->counts[type]++;

	if( amt & (HAT_cache_line - 1) )
		amt |= (HAT_cache_line - 1), amt += 1;

	//	see if free block is already available

	if( (block = hat->reuse[type]) ) {
		hat->reuse[type] = *(void **)block;
		memset (block, 0, amt);
		return (void *)block;
	}

	if( hat->seg->next + amt > HAT_seg ) {
		if( (seg = malloc (HAT_seg)) ) {
			seg->next = sizeof(*seg);
			seg->seg = hat->seg;
			hat->seg = seg;
			if( round = (HatSlot)seg & (HAT_cache_line - 1) )
				seg->next += HAT_cache_line - round;
		} else {
			hat_abort("Out of virtual memory");
		}

		MaxMem += HAT_seg;
	}

	block = (void *)((uchar *)hat->seg + hat->seg->next);
	hat->seg->next += amt;
	memset (block, 0, amt);

	return block;
}

void *hat_data (Hat *hat, uint amt)
{
HatSeg *seg;
void *block;
uint round;

	if( amt & (HAT_cache_line - 1))
		amt |= (HAT_cache_line - 1), amt += 1;

	if( hat->seg->next + amt > HAT_seg ) {
		if( (seg = malloc (HAT_seg)) ) {
			seg->next = sizeof(*seg);
			seg->seg = hat->seg;
			hat->seg = seg;
			if( round = (HatSlot)seg & (HAT_cache_line - 1) )
				seg->next += HAT_cache_line - round;
		} else {
			hat_abort("Out of virtual memory");
		}
	
		MaxMem += HAT_seg;
	}

	block = (void *)((uchar *)hat->seg + hat->seg->next);
	hat->seg->next += amt;
	memset (block, 0, amt);

	return block;
}

void hat_free (Hat *hat, void *block, int type)
{
	*((void **)(block)) = hat->reuse[type];
	hat->reuse[type] = (void **)block;
	hat->counts[type]--;
	return;
}
		
//	open hat object
//	call with number of radix levels to boot into root
//	and number of auxilliary user bytes to assign to each key

void *hat_open (int boot, int aux)
{
uint amt, size = HAT_slot_size, round;
HatSeg *seg;
Hat *hat;
int idx;

	for( idx = 0; idx < boot; idx++ )
		size *= 128;

	amt = sizeof(Hat) + size;

	if( amt & (HAT_cache_line - 1) )
		amt |= HAT_cache_line - 1, amt++;

	if( (seg = malloc(amt + HAT_seg)) ) {
		seg->next = sizeof(*seg);
		seg->seg = NULL;
		if( round = (HatSlot)seg & (HAT_cache_line - 1) )
			seg->next += HAT_cache_line - round;
	} else {
		hat_abort ("No virtual memory");
	}

	MaxMem += amt + HAT_seg;

	hat = (Hat *)((uchar *)seg + HAT_seg);

	memset(hat, 0, amt);
	hat->bootlvl = boot;
 	hat->aux = aux;
 	hat->seg = seg;

	if( !boot )
		*hat->root = (HatSlot)hat_alloc (hat, HAT_bucket) | HAT_bucket;

	return hat;
}

void hat_close (Hat *hat)
{
HatSeg *seg, *nxt = hat->seg;

	while( (seg = nxt) )
		nxt = seg->seg, free (seg);
}

//	compute hash code for key

uint hat_code (uchar *buff, uint max)
{
uint hash = max;

	while( max-- )
		hash += (hash << 5) + (hash >> 27) + *buff++;

	return hash;
}

void *hat_add_array (Hat *hat, HatSlot *parent, uchar *buff, uint amt, int pail);
void *hat_new_array (Hat *hat, HatSlot *parent, uchar *buff, uint amt);

//	add new key to existing HAT_pail node
//	return auxilliary area pointer, or
//	NULL if it doesn't fit PAIL array

void *hat_add_pail (Hat *hat, HatSlot *parent, uchar *buff, uint amt)
{
HatPail *pail = (HatPail *)(*parent & HAT_mask);
uint slot = hat_code (buff, amt) % HatPailMax;
void *cell;

	if( !pail->array[slot] )
		return hat_new_array (hat, &pail->array[slot], buff, amt);

	//	does room exist in slot?

	if( cell = hat_add_array (hat, &pail->array[slot], buff, amt, 0) )
		return cell;

	return NULL;
}

//	create new HAT_pail node
//	from full HAT array node
//	by bursting it

void *hat_new_pail (Hat *hat, HatSlot *parent, uchar *buff, uint amt)
{
HatBase *base = (HatBase *)(*parent & HAT_mask);
ushort tst = 0, len, cnt = 0;
HatPail *pail;
uchar *cell;
uint code;

	// strip array node keys into HAT_pail structure

	pail = hat_alloc (hat, HAT_pail);
	*parent = (HatSlot)pail | HAT_pail;

	//	burst array node into new PAIL node

	while( tst < base->nxt ) {
	  len = base->keys[tst++];

	  if( len & 0x80 )
		len &= 0x7f, len += base->keys[tst++] << 7;

	  code = hat_code (base->keys + tst, len) % HatPailMax;

	  if( pail->array[code] ) {
		cell = hat_add_array (hat, &pail->array[code], base->keys + tst, len, 0);
		if( hat->aux )
			memcpy(cell, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux, hat->aux);
	  } else {
		cell = hat_new_array (hat, &pail->array[code], base->keys + tst, len);
		if(  hat->aux )
			memcpy (cell, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux, hat->aux);
	  }

	  tst += len;
	  cnt++;
	}

	hat_free (hat, base, base->type);
	return hat_add_pail (hat, parent, buff, amt);
}

//	promote full array nodes to next larger size
//	if configured, overflow to HAT_pail node

void *hat_promote (Hat *hat, HatSlot *parent, uchar *buff, int amt, int pail)
{
HatBase *base = (HatBase *)(*parent & HAT_mask);
uchar *oldslots, *newslots;
ushort tst, len, skip;
uint type, oldtype;
HatBase *newbase;

	if( amt > 0x7f )
		skip = 2;
	else
		skip = 1;

	oldtype = type = base->type;
	oldslots = (uchar *)base + HatSize[type];

	//	calculate new array node big enough to contain keys
	//	and associated slots

	if( !hat->aux || base->cnt < 255 )
	  do if( (base->cnt + 1) * hat->aux + base->nxt + amt + skip + sizeof(HatBase) > HatSize[type] )
		continue;
	   else
		break;
	  while( type++ < HatMax );
	else
	  type = HatMax + 1;

	//  see if new key fits into largest array
	//	if not, promote to HAT_pail as configured

	if( type > HatMax )
	  if( pail && HatPailMax )
		return hat_new_pail (hat, parent, buff, amt);
	  else
		return NULL;

	// promote node to next larger size

	newbase = hat_alloc (hat, type);
	*parent = (HatSlot)newbase | HAT_array;
	newslots = (uchar *)newbase + HatSize[type];

	//	copy old node contents

	memcpy (newbase->keys, base->keys, base->nxt);	// copy keys in node

	if( hat->aux )
		memcpy (newslots - base->cnt * hat->aux, oldslots - base->cnt * hat->aux, base->cnt * hat->aux);	//	copy user slots

	//	append new node

	tst = base->nxt;
	newbase->keys[tst] = amt & 0x7f;

	if( amt & 0x80 )
		newbase->keys[tst] |= 0x80, newbase->keys[tst + 1] = amt >> 7;

	memcpy (newbase->keys + tst + skip, buff, amt);

	newbase->nxt = tst + amt + skip;
	newbase->cnt = base->cnt + 1;
	newbase->type = type;

	hat_free (hat, base, oldtype);
	return newslots - newbase->cnt * hat->aux;
}

//	make new hat array node
//	to contain new key
//	guaranteed to fit

void *hat_new_array (Hat *hat, HatSlot *parent, uchar *buff, uint amt)
{
uint type = HAT_1;
HatBase *base;
ushort skip;

	if( amt > 0x7f )
		skip = 2;
	else
		skip = 1;

	while( hat->aux + amt + skip + sizeof(HatBase) > HatSize[type] )
		type++;

	//	new key doesn't fit into largest array

	if( type > HatMax )
		return NULL;

	base = hat_alloc (hat, type);
	*parent = (HatSlot)base | HAT_array;

	base->keys[0] = amt & 0x7f;

	if( amt > 0x7f )
		base->keys[0] |= 0x80, base->keys[1] = amt >> 7;

	memcpy (base->keys + skip, buff, amt);
	base->nxt = amt + skip;
	base->type = type;
	base->cnt = 1;
	return (uchar *)base + HatSize[type] - hat->aux;
}

//	add to existing hat array node

//	return slot address
//	  or NULL if it doesn't fit

void *hat_add_array (Hat *hat, HatSlot *parent, uchar *buff, uint amt, int pail)
{
HatBase *base;
ushort skip;
uint type;

	if( amt > 0x7f )
		skip = 2;
	else
		skip = 1;

	base = (HatBase *)(*parent & HAT_mask);
	type = base->type;

	// add key to existing array

	if( !hat->aux || base->cnt < 255 )
	  if( (base->cnt + 1 ) * hat->aux + base->nxt + amt + skip + sizeof(HatBase) <= HatSize[type] ) {
		memcpy (base->keys + base->nxt + skip, buff, amt);
		base->keys[base->nxt] = amt & 0x7f;
		if( amt > 0x7f )
			base->keys[base->nxt] |= 0x80, base->keys[base->nxt + 1] = amt >> 7;
		base->nxt += amt + skip;
		base->cnt++;
		return (uchar *)base + HatSize[type] - base->cnt * hat->aux;
	  }

	return hat_promote (hat, parent, buff, amt, pail);
}

//	burst full array node into new bucket node

void hat_burst_array (Hat *hat, HatSlot *parent)
{
ushort tst, len, type, cnt;
HatBucket *bucket;
HatBase *base;
uchar *cell;
uint code;

	base = (HatBase *)(*parent & HAT_mask);
	type = base->type;
	cnt = tst = 0;

	//	allocate new bucket node

	bucket = hat_alloc (hat, HAT_bucket);
	*parent = (HatSlot)bucket | HAT_bucket;

	//	burst array node into new bucket node

	while( tst < base->nxt ) {
	  len = base->keys[tst++];
	  if( len > 0x7f )
		len &= 0x7f, len += base->keys[tst++] << 7;

	  code = hat_code (base->keys + tst, len) % HatBucketSlots;

	  if( bucket->slots[code] ) {
		cell = hat_add_array (hat, &bucket->slots[code], base->keys + tst, len, 1);
		if( hat->aux )
		  memcpy (cell, (uchar *)base + HatSize[type] - (cnt + 1) * hat->aux, hat->aux);
	  } else {
		cell = hat_new_array (hat, &bucket->slots[code], base->keys + tst, len);
		if( hat->aux )
		  memcpy (cell, (uchar *)base + HatSize[type] - (cnt + 1) * hat->aux, hat->aux);
	  }

	  bucket->count++;
	  tst += len;
	  cnt++;
	}

	hat_free (hat, base, type);
}

//	burst overflowing HAT_pail hash table into HAT_bucket hash table

void hat_burst_pail (Hat *hat, HatSlot *parent)
{
HatPail *pail = (HatPail *)(*parent & HAT_mask);
ushort tst, len, type, cnt, idx;
HatBucket *bucket;
HatBase *base;
uchar *cell;
uint code;

	//	allocate new bucket node

	bucket = hat_alloc (hat, HAT_bucket);
	*parent = (HatSlot)bucket | HAT_bucket;

	//	burst pail array into new bucket node

	for( idx = 0; idx < HatPailMax; idx++ ) {
	 base = (HatBase *)(pail->array[idx] & HAT_mask);
	 if( !base )
		continue;

	 cnt = tst = 0;

	 while( tst < base->nxt ) {
	  len = base->keys[tst++];

	  if( len & 0x80 )
		len &= 0x7f, len += base->keys[tst++] << 7;

	  code = hat_code (base->keys + tst, len) % HatBucketSlots;

	  if( bucket->slots[code] ) {
		if( (bucket->slots[code] & HAT_type) == HAT_array ) {
		  cell = hat_add_array (hat, &bucket->slots[code], base->keys + tst, len, 1);
		  if( hat->aux )
			memcpy (cell, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux, hat->aux);
		} else {
		  cell = hat_add_pail (hat, &bucket->slots[code], base->keys + tst, len);
		  if( hat->aux )
			memcpy (cell, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux, hat->aux);
		}
	  } else {
		  cell = hat_new_array (hat, &bucket->slots[code], base->keys + tst, len);
		  if( hat->aux )
			memcpy (cell, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux, hat->aux);
	  }

	   bucket->count++;
	   tst += len;
	   cnt++;
	 }

	 hat_free (hat, base, base->type);
	}
   hat_free (hat, pail, HAT_pail);
}

//	add key to HAT_bucket node

//	return 1 on success
//	  or 0 if bucket overflows

int hat_add_bucket (Hat *hat, HatSlot *parent, uchar *buff, uint amt, uchar *value)
{
HatBucket *bucket;
uchar *cell;
uint code;

	bucket = (HatBucket *)(*parent & HAT_mask);
	code = hat_code (buff, amt) % HatBucketSlots;

	if( bucket->count++ < HatBucketMax )
	 if( !bucket->slots[code] ) {
	  cell = hat_new_array (hat, &bucket->slots[code], buff, amt);
	  if( hat->aux )
		memcpy (cell, value, hat->aux);
	  return 1;
	 } else if( (bucket->slots[code] & HAT_type) == HAT_array ) {
	  if( cell = hat_add_array (hat, &bucket->slots[code], buff, amt, 1) ) {
	    memcpy (cell, value, hat->aux);
		return 1;
	  } else
		return 0;
	 } else
	  if( cell = hat_add_pail (hat, &bucket->slots[code], buff, amt) ) {
	    memcpy (cell, value, hat->aux);
	    return 1;
	  } else
		return 0;

	return 0;
}

void hat_burst_bucket (Hat *hat, HatSlot *parent);

//	burst HAT_bucket node node into HAT_radix entry
//	moving key over one offset

void hat_add_radix (Hat *hat, HatSlot *radix, uchar *buff, uint max, uchar *value)
{
void *cell;
uchar ch;

  //  shorten key by 1 byte

  if( max )
	ch = buff[0];
  else
	ch = 0;

  //  if radix slot is empty, create new HAT_array node

  if( !radix[ch] ) {
	cell = hat_new_array (hat, &radix[ch], buff + 1, max ? max - 1 : 0);
	if( hat->aux )
		memcpy (cell, value, hat->aux);
	return;
  }

  //  otherwise, add to existing node

  do switch( radix[ch] & HAT_type ) {
	case HAT_bucket:
	  if( hat_add_bucket (hat, &radix[ch], buff + 1, max - 1, value) )
		return;

	  hat_burst_bucket (hat, &radix[ch]);
	  continue;

	case HAT_radix:
	  radix = (HatSlot *)(radix[ch] & HAT_mask);
	  hat_add_radix (hat, radix, buff + 1, max - 1, value);
	  return;

	case HAT_array:
	  if( cell = hat_add_array (hat, &radix[ch], buff + 1, max - 1, 1) ) {
		if( hat->aux )
			memcpy (cell, value, hat->aux);
		return;
	  }

	  hat_burst_array (hat, &radix[ch]);
	  continue;

	case HAT_pail:
	  if( cell = hat_add_pail (hat, &radix[ch], buff + 1, max - 1) ) {
		if( hat->aux )
			memcpy (cell, value, hat->aux);
		return;
	  }

	  hat_burst_pail (hat, &radix[ch]);
	  continue;
  } while( 1 );
}

//	decompose full bucket to radix node

void hat_burst_bucket (Hat *hat, HatSlot *parent)
{
HatPail *pail, *chain;
HatBucket *bucket;
HatSlot *radix;
HatBase *base;
uint hash, idx;
ushort tst, cnt;
uchar len;

  bucket = (HatBucket *)(*parent & HAT_mask);

  if( bucket->count < HatBucketMax )
	Small++;

  //	allocate new hat_radix node

  radix = hat_alloc (hat, HAT_radix);
  *parent = (HatSlot)radix | HAT_radix;

  for( hash = 0; hash < HatBucketSlots; hash++ )
   if( bucket->slots[hash] )
    switch( bucket->slots[hash] & HAT_type ) {
    case HAT_array:
	  base = (HatBase *)(bucket->slots[hash] & HAT_mask);
	  cnt = tst = 0;

	  while( tst < base->nxt ) {
		len = base->keys[tst++];
		if( len > 0x7f )
			len &= 0x7f, len += base->keys[tst++] << 7;
		hat_add_radix (hat, radix, base->keys + tst, len, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux);
		tst += len;
		cnt++;
	  }

	  hat_free (hat, base, base->type);
	  continue;

	case HAT_pail:
	  pail = (HatPail *)(bucket->slots[hash] & HAT_mask);

	  for( idx = 0; idx < HatPailMax; idx++ ) {
	    base = (HatBase *)(pail->array[idx] & HAT_mask);

		if( !base )
		  continue;

  		cnt = tst = 0;

		while( tst < base->nxt ) {
		  len = base->keys[tst++];

		  if( len > 0x7f )
			len &= 0x7f, len += base->keys[tst++] << 7;

		  hat_add_radix (hat, radix, base->keys + tst, len, (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux);
		  tst += len;
		  cnt++;
		}

		hat_free (hat, base, base->type);
	  }
	  hat_free (hat, pail, HAT_pail);
	}

  hat_free (hat, bucket, HAT_bucket);
}

int keycmp (uchar *str1, uchar *str2, uint len)
{
	while( len & (HAT_slot_size - 1) )
	  if( len--, str1[len] != str2[len] )
		return 1;

	while( len )
	  if( *(HatSlot *)str1 != *(HatSlot *)str2 )
		return 1;
	  else {
		str1 += HAT_slot_size;
		str2 += HAT_slot_size;
		len -= HAT_slot_size;
	  }

	return 0;
}

//	hat_find: find string in hat array
//	returning a pointer to associated slot

void *hat_find (Hat *hat, uchar *buff, uint max)
{
HatSlot next, *table;
HatBucket *bucket;
HatBase *base;
HatPail *pail;
ushort tst, cnt;
uint triple = 0;
uint code, len;
uint off = 0;
uchar ch;

  for( tst = 0; tst < hat->bootlvl; tst++ ) {
	triple *= 128;
	if( off < max )
	  triple += buff[off++];
  }

  next = hat->root[triple];

  while( next )
	switch( next & HAT_type ) {
	case HAT_array:
	  base = (HatBase *)(next & HAT_mask);
	  cnt = tst = 0;
	  Searches++;

	  //  find slot == key

	  while( tst < base->nxt ) {
		Probes++;
		len = base->keys[tst++];	// key length

		if( len > 0x7f )
			len += base->keys[tst++] << 7;

		if( len == max - off )
		  if( !keycmp (base->keys + tst, buff + off, len) )
			if( hat->aux )
			  return (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux;
			else
			  return (void *)1;
		tst += len;
		cnt++;
	  }

	  return NULL;

	case HAT_pail:
	  pail = (HatPail *)(next & HAT_mask);
	  Pail++;

	  code = hat_code (buff + off, max - off) % HatPailMax;

	  if( next = pail->array[code] )
		continue;

	  return NULL;

	case HAT_bucket:
	  bucket = (HatBucket *)(next & HAT_mask);
	  Bucket++;

	  code = hat_code (buff + off, max - off) % HatBucketSlots;

	  if( next = bucket->slots[code] )
		continue;

	  return NULL;

	case HAT_radix:
	  table = (HatSlot *)(next & HAT_mask);
	  Radix++;

	  if( off < max )
		ch = buff[off++];
	  else
		ch = 0;

	  next = table[ch];
	  continue;
	}

	return NULL;
}

//	hat_cell: add string to hat array
//	returning address of associated slot

void *hat_cell (Hat *hat, uchar *buff, uint max)
{
HatSlot *table, *next, *parent, node;
HatBucket *bucket;
HatBase *base;
HatPail *pail;
ushort tst, cnt;
uint triple = 0;
uint len, code;
uint off = 0;
void *cell;
uchar ch;

  for( tst = 0; tst < hat->bootlvl; tst++ ) {
	triple *= 128;
	if( off < max )
	  triple += buff[off++];
  }

  next = &hat->root[triple];
  parent = NULL;

loop:
  while( node = *next )
	switch( node & HAT_type ) {
	case HAT_array:
	  base = (HatBase *)(node & HAT_mask);
	  cnt = tst = 0;

	  //  find slot == key

	  while( tst < base->nxt ) {
		len = base->keys[tst++];	// key length

		if( len > 0x7f )
			len += base->keys[tst++] << 7;

		if( len == max - off )
		  if( !keycmp (base->keys + tst, buff + off, max - off) )
			if( hat->aux )
			  return (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux;
			else
			  return (void *)1;

		tst += len;
		cnt++;
	  }

	  //  if parent node is a full bucket node,
	  //  burst it and loop to reprocess insert

	  if( parent ) {
		if( bucket->count++ < HatBucketMax )
		  if( cell = hat_add_array (hat, next, buff + off, max - off, 1) )
			if( hat->aux )
			  return cell;
			else
			  return (void *)0;

		hat_burst_bucket (hat, parent);
		next = parent;
		parent = NULL;
		continue;
	  }

	  // add new key to existing array or create new pail array node

	  if( cell = hat_add_array (hat, next, buff + off, max - off, 1) )
		if( hat->aux )
		  return cell;
		else
		  return (void *)0;

	  //  burst full array node into HAT_bucket node
	  //  and loop to reprocess the insert

	  hat_burst_array (hat, next);
	  continue;

	case HAT_pail:
	  pail = (HatPail *)(node & HAT_mask);

	  //  find slot == key

	  cnt = tst = 0;
	  code = hat_code (buff + off, max - off) % HatPailMax;

	  if( base = (HatBase *)(pail->array[code] & HAT_mask) )
	    while( tst < base->nxt ) {
		 len = base->keys[tst++];	// key length

		 if( len > 0x7f )
		  len += base->keys[tst++] << 7;

		 if( len == max - off )
		  if( !keycmp (base->keys + tst, buff + off, max - off) )
			if( hat->aux )
			  return (uchar *)base + HatSize[base->type] - (cnt + 1) * hat->aux;
			else
			  return (void *)1;

		 tst += len;
		 cnt++;
	    }

	  //  if parent node is a full bucket node,
	  //  burst it and loop to reprocess insert

	 if( parent ) {
		if( bucket->count++ < HatBucketMax )
		  if( cell = hat_add_pail (hat, next, buff + off, max - off) )
			if( hat->aux )
			  return cell;
			else
			  return (void *)0;

		hat_burst_bucket (hat, parent);
		next = parent;
		parent = NULL;
		continue;
	  }

	  if( cell = hat_add_pail (hat, next, buff + off, max - off) )
		if( hat->aux )
		  return cell;
		else
		  return (void *)0;

	  //  burst full pail node into HAT_bucket node
	  //  and loop to reprocess the insert

	  hat_burst_pail (hat, next);
	  continue;

	case HAT_bucket:
	  bucket = (HatBucket *)(node & HAT_mask);
	  code = hat_code (buff + off, max - off) % HatBucketSlots;

	  parent = next;
	  next = &bucket->slots[code];
	  continue;

	case HAT_radix:
	  table = (HatSlot *)(node & HAT_mask);

	  if( off < max )
	  	ch = buff[off++];
	  else
	  	ch = 0;

	  next = &table[ch];
	  continue;
	}

	// place new array node under HAT_bucket
	//	loop if bucket overflows

	if( parent )
	  if( bucket->count++ < HatBucketMax ) {
	   if( cell = hat_new_array (hat, next, buff + off, max - off) )
		if( hat->aux )
		  return cell;
		else
		  return (void *)0;

	   hat_burst_bucket (hat, parent);
	   next = parent;
	   parent = NULL;
	   goto loop;
	}

	// place new array node under HAT_radix

	cell = hat_new_array (hat, next, buff + off, max - off);

	if( hat->aux )
		return cell;

	return (void *)0;
}

//	demonstration sort program

void sorthattrie (int lvl, FILE *in)
{
Hat *hat = hat_open (lvl, sizeof(uint));
uchar buff[256];
void *cursor;
uint *cell;
uint max;
	
	while( fgets (buff, sizeof(buff), in) ) {
		max = strlen(buff);
		while( max-- )
			buff[max] &= 0x7f;
		cell = hat_cell (hat, buff, strlen(buff) - 1);
		*cell += 1;
	}

	cursor = hat_cursor (hat);

#ifndef REVERSE
	if( hat_start (cursor, NULL, 0) )
	  do {
		hat_key (cursor, buff, sizeof(buff));
		cell = hat_slot (cursor);
		max = *cell;
		while( max-- )
			puts (buff);
	  } while( hat_nxt (cursor) );
#else
	if( hat_last (cursor) )
	  do {
		hat_key (cursor, buff, sizeof(buff));
		cell = hat_slot (cursor);
		max = *cell;
		while( max-- )
			puts (buff);
	  } while( hat_prv (cursor) );
#endif
	if( cursor )
		free (cursor);

	exit(0);
}

#if defined(__APPLE__) || defined(linux)
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/times.h>
#else
#include <windows.h>
#include <io.h>
#endif

#include <time.h>

#if !defined(_WIN32)
unsigned long long rd_clock ()
{
unsigned int low, high;

	__asm__ volatile("rdtsc" : "=a"(low), "=d" (high)); 
	return (unsigned long long)low | (unsigned long long)high << 32;
}
#endif
#if !defined(_WIN32)
// naskitis.com:
// This function will report the actual process size.
// note: this many not work on an Apple OS linux console.

typedef struct timeval timer;

unsigned long long report_process_size(void)
{
  FILE * statf;
  char fname[1024];
  char commbuf[1024];
  char state;
  pid_t mypid;
  unsigned long long vsize=0;
  unsigned int ppid, pgrp, session, ttyd, tpgid, flags, minflt, cminflt, majflt, cmajflt;
  unsigned int utime, stime, cutime, cstime, counter, priority, timeout, itrealvalue;
  unsigned int starttime, rss, rlim, startcode, endcode, startstack, kstkesp, ksteip;
  unsigned int signal, blocked, sigignore, sigcatch, wchan, ret, pid;
 
  mypid = getpid();
  snprintf(fname, 1024, "/proc/%u/stat", mypid);
  statf = fopen(fname, "r");
  ret = fscanf(statf, "%lu %s %c %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu "
                      "%lu %llu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
               &pid, commbuf, &state, &ppid, &pgrp, &session, &ttyd, &tpgid, &flags, &minflt, &cminflt, &majflt,
               &cmajflt, &utime, &stime, &cutime, &cstime, &counter, &priority, &timeout, &itrealvalue,
               &starttime, &vsize, &rss, &rlim, &startcode, &endcode, &startstack, &kstkesp, &ksteip, &signal,
               &blocked, &sigignore, &sigcatch, &wchan);
 
  if (ret != 35)
     fprintf(stderr, "Failed to read all 35 fields, only %lu decoded\n", ret);
 
  fclose(statf);
  return vsize; 
}
#endif

//	naskitis.com.
//	g++ -O3 -fpermissive -fomit-frame-pointer -w -o askitis2 askitis2.c
//	./askitis [input-file-to-build-hat] e.g. distinct_1 or skew1_1 [input-file-to-search-hat]

//	note:  the hat array is an in-memory data structure. As such, make sure you
//	have enough memory to hold the entire input file + data structure, otherwise
//	you'll have to break the input file into smaller pieces and load them in 
//	on-by-one. 

int Words = 0;
int Inserts = 0;
int Missing = 0;
int Found = 0;

int main (int argc, char **argv)
{
FILE *in, *in2;
Hat *hat;
char *askitis;
int idx = HAT_1 - 1;
int boot = 3;
HatSlot *cell;

double insert_real_time=0.0;
double search_real_time=0.0;
unsigned long long size, off, prev;
#if !defined(_WIN32)
timer start, stop;
#else
clock_t start[1], stop[1];
#endif
unsigned long long startcycles, stopcycles;

	if( argc > 1 )
		in = fopen (argv[1], "rb");
	else
		in = NULL;

	if( argc > 2 && strlen(argv[2]) > 0 )
		in2 = fopen (argv[2], "rb");
	else
		in2 = NULL;

	if( !in )
		fprintf (stderr, "unable to open input file #1\n");

	if( argc > 3 )
		boot = atoi(argv[3]);

	if( argc > 4 )
		HatPailMax = atoi(argv[4]);

	if( argc > 5 )
		HatBucketSlots = atoi(argv[5]);

	if( argc > 6 )
		HatBucketMax = atoi(argv[6]);

	while( argc > 7 && ++idx < 32 ) {
	 	HatSize[idx] = atoi(argv[7]) * HAT_node_size;
		argv++;
		argc--; 
	}

	if( idx > 31 )
		fprintf (stderr, "Too many block sizes\n"), idx = 31;

	if( idx >= HAT_1 )
		HatMax = idx;

	HatSize[HAT_bucket] += HatBucketSlots * HAT_slot_size;
	HatSize[HAT_pail] += HatPailMax * HAT_slot_size;

	if( !in2 )
		sorthattrie (boot, in);

//	build hat array
	hat = hat_open (boot, 0);

#if !defined(_WIN32)
	size = lseek (fileno(in), 0L, 2);
	askitis = malloc(size);
	lseek (fileno(in), 0L, 0);
#else
	size = _lseeki64 (fileno(in), 0L, 2);
	askitis = malloc(size);
	_lseeki64 (fileno(in), 0L, 0);
#endif
	off = 0;

	do {
	  prev = read (fileno(in), askitis+off,size-off > 65536 ? 65536 : size-off);
	  off += prev;
	} while( off < size );

//	naskitis.com:
//	Start the timer. 
	
#if !defined(_WIN32)
	gettimeofday(&start, NULL);
	startcycles = rd_clock();
#else
	QueryProcessCycleTime(GetCurrentProcess(), &startcycles);
	*start = clock();
#endif

	for( prev = off = 0; off < size; off++ )
	  if( askitis[off] == '\n' ) {
		Words++;
		if( hat_cell (hat, askitis+prev, off - prev) )
			Found++;
		else
			Inserts++;
		prev = off + 1;
	  }

//	naskitis.com:
//	Stop the timer and do some math to compute the time required to insert the strings into the hat array.

#if !defined(_WIN32)
	stopcycles = rd_clock();
	gettimeofday(&stop, NULL);
	
	insert_real_time = 1000.0 * ( stop.tv_sec - start.tv_sec ) + 0.001 * (stop.tv_usec - start.tv_usec );
	insert_real_time = insert_real_time/1000.0;
#else
	*stop = clock();
	QueryProcessCycleTime(GetCurrentProcess(), &stopcycles);
	insert_real_time = (*stop - *start) / (float)CLOCKS_PER_SEC;
#endif

//	naskitis.com:
//	Free the input buffer used to store the first file.  We must do this before we get the process size below. 
	free (askitis);
	fprintf(stderr, "HatArray@Karl_Malbrain\nDASKITIS option enabled\n-------------------------------\n%-20s %.2f MB\n%-20s %.2f sec\n",
    "Hat Array size:", MaxMem/1000000., "Time to insert:", insert_real_time);
#if !defined(_WIN32)
	fprintf(stderr, "%-20s %.2f MB\n", "Process Size:", report_process_size()/1000000.);
#endif
	fprintf(stderr, "%-20s %d\n", "Words:", Words);
	fprintf(stderr, "%-20s %d\n", "Inserts:", Inserts);
	fprintf(stderr, "%-20s %d\n", "Found:", Found);
	fprintf(stderr, "%-20s %d\n", "Cycles/Insert", (stopcycles - startcycles)/Words);
	fprintf(stderr, "%-20s %d\n", "Short Bucket:", Small);
	fprintf(stderr, "%-20s %d\n", "Radix Nodes:", hat->counts[0]);
	fprintf(stderr, "%-20s %d\n", "Bucket Nodes:", hat->counts[1]);
	fprintf(stderr, "%-20s %d\n", "Pail Nodes:", hat->counts[3]);

	for( idx = 4; idx <= HatMax; idx++ )
	  fprintf(stderr, "HAT_%.4d Nodes:      %d\n", HatSize[idx], hat->counts[idx]);

	Words = 0;
	Probes = 0;
	Searches = 0;
	Pail = 0;
	Bucket = 0;
	Inserts = 0;
	Missing = 0;
	Found = 0;

//	search hat array

#if !defined(_WIN32)
	size = lseek (fileno(in2), 0L, 2);
	askitis = malloc(size);
	lseek (fileno(in2), 0L, 0);
#else
	size = _lseeki64 (fileno(in2), 0L, 2);
	askitis = malloc(size);
	_lseeki64 (fileno(in2), 0L, 0);
#endif
	off = 0;

	while( off < size ) {
	  prev = read (fileno(in2), askitis+off,size-off > 65536 ? 65536 : size-off);
	  off += prev;
	}

#if !defined(_WIN32)
	gettimeofday(&start, NULL);
	startcycles = rd_clock();
#else
	QueryProcessCycleTime(GetCurrentProcess(), &startcycles);
	*start = clock();
#endif
	for( prev = off = 0; off < size; off++ )
	  if( askitis[off] == '\n' ) {
		Words++;
		if( hat_find (hat, askitis+prev, off - prev) )
			Found++;
		else
			Missing++;
		prev = off + 1;
	  }

//	naskitis.com:
//	Stop the timer and do some math to compute the time required to search the hat array.

#if !defined(_WIN32)
	gettimeofday(&stop, NULL);
	search_real_time = 1000.0 * ( stop.tv_sec - start.tv_sec ) + 0.001  
	* (stop.tv_usec - start.tv_usec );
	search_real_time = search_real_time/1000.0;
	stopcycles = rd_clock();
#else	
	QueryProcessCycleTime(GetCurrentProcess(), &stopcycles);
	*stop = clock ();
	search_real_time = (*stop - *start) / (float)CLOCKS_PER_SEC;
#endif

	free (askitis);

	fprintf(stderr,"\n%-20s %.2f sec\n", "Time to search:", search_real_time);
	fprintf(stderr, "%-20s %d\n", "Words:", Words);
	fprintf(stderr, "%-20s %d\n", "Missing:", Missing);
	fprintf(stderr, "%-20s %d\n", "Found:", Found);
	fprintf(stderr, "%-20s %d\n", "Cycles/Search", (stopcycles - startcycles)/Words);
	fprintf(stderr, "%-20s %.2f\n", "nSec/Search:", 1000000000. * search_real_time / Words);
	fprintf(stderr, "%-20s %.2f\n", "Probes/Array:", (double)Probes / Searches);
	fprintf(stderr, "%-20s %.2f\n", "Pail/Search:", (double)Pail / Searches);
	fprintf(stderr, "%-20s %.2f\n", "Bucket/Search:", (double)Bucket / Words);
	fprintf(stderr, "%-20s %.2f\n", "Radix/Search:", (double)Radix / Words);

	exit(0);
}

