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

#ifndef __CONTEXT_MANAGER_H_
#define __CONTEXT_MANAGER_H_

#include <list>
#include <vector>


#define CM_DEFAULT_NUM_THREADS	8
#define CM_DEFAULT_QDB_KEY_SIZE	64
#define CM_DEFAULT_QDB_VALUE_SIZE	64
#define CM_DEFAULT_MD_LOOKUP_VALUE_SIZE	16
#define CM_DEFAULT_CACHEDB_KEY_SIZE	64
#define CM_DEFAULT_MD_LOOKUP_KEY_SIZE	64

#define CM_INITIALIZE_NUM_LISTS	4
#define CM_INITIALIZE_LIST_LENGTH 4
#define CM_MAX_LIST_LENGTH	8

struct ContextStructure
{
	char*	qdb_key;
	int		qdb_key_size;
	char*	qdb_value;
	int		qdb_value_size;
	char*	cachedb_key;
	int		cachedb_key_size;
	char*	md_lookup_key;
	int		md_lookup_key_size;
	char*	md_lookup_value;
	int		md_lookup_value_size;
	bool	free;
	int		list_id;
};


struct FreeList
{
	std::list<ContextStructure*> freeStack;
	CRITICAL_SECTION	crit_section;
	int		length;
};


class _export ContextManager
{
private:
	int m_threadCount;
	ContextStructure* m_contextArray;

	int m_init_num_lists;
	int m_init_list_length;
	int m_max_list_length;

	int m_numFreeLists;
	std::vector<FreeList*> m_listOfFreeLists;
	CRITICAL_SECTION	m_critSectionForListAppends;

	void init(	const int& init_num_lists,
				const int& init_list_length,
				const int& max_list_length );
	int	CreateNewContext( ContextStructure* &new_context );
	int CreateNewFreeList();
	int TryEnterAndCheck( const int& list_id );
	FreeList* getIndexElement( const int& list_id );

	// Debug counters
	CRITICAL_SECTION	m_debug_criticalSection;
	int m_debug_numAccesses;
	double m_debug_totalNumTries;
	double m_debug_totalNumEnterTries;
	double m_debug_totalNumTimesNoElementsFree;
	double m_debug_totalNumTimesListFull;
	int m_debug_totalNumNew;

public:
	ContextManager();
	ContextManager( const int& init_num_lists,
					const int& init_list_length,
					const int& max_list_length );
	~ContextManager();

	// Acquire free context memory. 
	int GetFreeContext( ContextStructure* &free_struct );

	// Release context memory
	int ReleaseContext( ContextStructure* free_struct );
	
	// Performs realloc on mem_ptr, maintaining the old contents over in the new memory block
	static int ReallocMemory( char* &mem_ptr, const int& curr_size, const int& target_size );

	// Frees memory pointed to by mem_ptr and allocates a memory block of target_size
	// Location of new memory block is set in mem_ptr. Old contents in memory are lost
	static int ResizeMemory( char* &mem_ptr, const int& target_size );

	void printStats();

};


#endif // __CONTEXT_MANAGER_H_
