/*
 * Copyright (c) 2009
 *   TrojansLab.  All rights reserved.
 *   Contributors:  Shahram Ghandeharizadeh, Jason Yap, Showvick Kalra, Jorge Gonzalez, Igor Shvager.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1.  Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 * 2.  Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Redistributions in any form must be accompanied by information on
 *    how to obtain complete source code for the TCache software and any accompanying
 *    software that uses the TCache software.  The source code must either be
 *    included in the distribution or be available for no more than the cost
 *    of distribution plust a nominal fee, and must be freely redistributable
 *    under reasonable conditions.  For an executable file, complete source
 *    means the source code for all modules it contains.  It does not include
 *    source code for all modules it contains.  It does not include source
 *    code for modules or files that typically accompany the major components
 *    of the operating system on which the executable file runs.
 *
 * This software is provided by TrojansLab LLC and contributors AS IS and any express or
 * implied warranties, including, but not limited to, the implied warranties
 * of merchantability, fitness for a particular purpose, or non-infringement,
 * are disclaimed.  In no even shall TrojansLab LLC and contributors be liable for
 * any direct, indirect, incidental, speical, exemplary, or consequential damages
 * (including, but not limited to, procurement of substitute goods or services;
 * loss of use, data, or profits; or business interuption) however caused and or any
 * theory of liability, whether in contract, strict liability, or tort (including
 * negligence or otherwise) arising in any way out of the use of this software,
 * even if advised of the possibility of such damage.
 */

#pragma once
#include <Windows.h>
#include <Winbase.h>
#include "db_cxx.h"
#include "common.h"

#define FREECELL	0
#define MAX_RES_LOG 12
#define MAXRES		(1 << MAX_RES_LOG) /* available Resources */
#define MAXCRITICAL 3


class HashIdx
{
private:
	struct VElt
	{
		VElt()
		{
			key = NULL;
			length = 0;
			dbptr = NULL;
			flink = blink = NULL;
		}
		~VElt()
		{
			flink = blink = NULL;
		}
		char *key;
		int  length;
		Db   *dbptr;
		short numOfPartition;
		struct VElt *flink, *blink;
	};

	struct HashElt
	{
		HashElt()
		{
			concurrent = NULL;
			ptr = NULL;
		};
		CRITICAL_SECTION *concurrent;
		struct VElt *ptr;
	};

	struct ArrayElt
	{
		struct VElt *arrayptr;
		struct ArrayElt *next;
	};

	struct ArrayElt *MallocHead;
	struct VElt freenodes[MAXRES];
	struct HashElt hashtable[MAXRES];
	CRITICAL_SECTION crtl[MAXCRITICAL];
	CRITICAL_SECTION protect_malloc;
	
	int h_hash(register const char *key, register int length);
	void first_init_res_node(VElt *, VElt *, VElt *);
	void init_res_node(VElt *, VElt *, VElt *, const char *, int, Db *DbHandle,short numOfPartitions);
	void init_resources(void);
	void alloc_more_node(void);
	struct VElt* malloc_node(void);
	void free_bucket(VElt *cell);
	void release_node(VElt *);
	void InitHashTable(void);
	struct VElt *HashIdx::findobj(const char *key, int length);
	struct VElt *insertobj(const char *, int, Db*,short);

	bool deleteobj(const char *key, int length);

public:
	Db  *Get(const char *key, int length,short &numOfPartitions);
	int Put(const char *key, int length, Db *,short numOfPartitions);
	int Delete(const char *key, int length);
	int DeleteOneKeyAndReturnDB(Db *&);
	HashIdx(void);
	~HashIdx(void);
};
