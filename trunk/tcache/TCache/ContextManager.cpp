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

#include "StdAfx.h"
#include "ContextManager.h"
#include <iostream>

ContextManager::ContextManager()
{
	/*
	//FreeList* free_list;
	//ContextStructure* context_struct = NULL;

	m_numFreeLists = 0;

	InitializeCriticalSection( &m_critSectionForListAppends );
	InitializeCriticalSection( &m_debug_criticalSection );

	m_debug_numAccesses = 0;
	m_debug_totalNumNew = 0;
	m_debug_totalNumTries = 0;
	m_debug_totalNumEnterTries = 0;
	m_debug_totalNumTimesListFull = 0;
	m_debug_totalNumTimesNoElementsFree = 0;


	for( int i = 0; i < CM_INITIALIZE_NUM_LISTS; i++ )
	{
		CreateNewFreeList();
	}
	//*/

	init( CM_INITIALIZE_NUM_LISTS, CM_INITIALIZE_LIST_LENGTH, CM_MAX_LIST_LENGTH );
}


ContextManager::ContextManager( const int& init_num_lists,
								const int& init_list_length,
								const int& max_list_length )
{
	init( init_num_lists, init_list_length, max_list_length );
}

void ContextManager::init(	const int& init_num_lists,
							const int& init_list_length,
							const int& max_list_length )
{
	//FreeList* free_list;
	//ContextStructure* context_struct = NULL;

	m_numFreeLists = 0;

	InitializeCriticalSection( &m_critSectionForListAppends );
	
	m_init_num_lists = init_num_lists;
	m_init_list_length = init_list_length;
	m_max_list_length = max_list_length;

	
	// Initialize debug counters
	InitializeCriticalSection( &m_debug_criticalSection );
	m_debug_numAccesses = 0;
	m_debug_totalNumNew = 0;
	m_debug_totalNumTries = 0;
	m_debug_totalNumEnterTries = 0;
	m_debug_totalNumTimesListFull = 0;
	m_debug_totalNumTimesNoElementsFree = 0;


	for( int i = 0; i < m_init_num_lists; i++ )
	{
		CreateNewFreeList();
	}
}

ContextManager::~ContextManager()
{
	// TODO: right now it assumes that all threads have released the resources back
	//       to the ContextManager

	std::vector<FreeList*>::iterator iter_top = m_listOfFreeLists.begin();
	std::list<ContextStructure*>::iterator iter_nested;

	while( iter_top != m_listOfFreeLists.end() )
	{
		iter_nested = (*iter_top)->freeStack.begin();
		while( iter_nested != (*iter_top)->freeStack.end() )
		{
			delete [] (*iter_nested)->qdb_key;
			delete [] (*iter_nested)->qdb_value;
			delete [] (*iter_nested)->cachedb_key;
			delete [] (*iter_nested)->md_lookup_key;
			delete [] (*iter_nested)->md_lookup_value;

			delete [] (*iter_nested);
			iter_nested++;
		}

		delete (*iter_top);
		iter_top++;
	}
}

int	ContextManager::CreateNewContext( ContextStructure* &new_context )
{
	try
	{
		new_context = new ContextStructure();
		if (new_context == NULL)
			printf("Error, failed to allocate a context structure.\n");

		new_context->qdb_key = new char[CM_DEFAULT_QDB_KEY_SIZE];
		new_context->qdb_key_size = CM_DEFAULT_QDB_KEY_SIZE;

		new_context->qdb_value = new char[CM_DEFAULT_QDB_VALUE_SIZE];
		new_context->qdb_value_size = CM_DEFAULT_QDB_VALUE_SIZE;

		new_context->cachedb_key = new char[CM_DEFAULT_CACHEDB_KEY_SIZE];
		new_context->cachedb_key_size = CM_DEFAULT_CACHEDB_KEY_SIZE;

		new_context->md_lookup_key = new char[CM_DEFAULT_MD_LOOKUP_KEY_SIZE];
		new_context->md_lookup_key_size = CM_DEFAULT_MD_LOOKUP_KEY_SIZE;

		new_context->md_lookup_value = new char[CM_DEFAULT_MD_LOOKUP_VALUE_SIZE];
		new_context->md_lookup_value_size = CM_DEFAULT_MD_LOOKUP_VALUE_SIZE;
	
		new_context->free = true;

		EnterCriticalSection( &m_debug_criticalSection );

		m_debug_totalNumNew += 6;

		LeaveCriticalSection( &m_debug_criticalSection );
	}
	catch(std::exception e)
	{
		return -1;
	}

	return 0;
}

int ContextManager::CreateNewFreeList()
{
	// Initialize FreeList
	FreeList* free_list = new FreeList();
	if (free_list == NULL)
		printf("Error, freelist is NULL in CreateNewFreeList.\n");

	EnterCriticalSection( &m_debug_criticalSection );

	m_debug_totalNumNew += 1;

	LeaveCriticalSection( &m_debug_criticalSection );
	
	InitializeCriticalSection( &(free_list->crit_section) );

	ContextStructure* context_struct;

	// Set the length first and fill later (so that if threads come in before we
	//  add the initial few ContextStructures, they will see an empty stack
	//  but won't add more than the MAX_LIST_LENGTH)
	free_list->length = m_init_list_length;

	EnterCriticalSection( &m_critSectionForListAppends );
	
	m_listOfFreeLists.push_back( free_list );
	int list_id = m_numFreeLists++;

	LeaveCriticalSection( &m_critSectionForListAppends );

	EnterCriticalSection( &free_list->crit_section );

	// Create Free Context Structures and add them to FreeList
	for( int i = 0; i < m_init_list_length; i++ )
	{
		context_struct = NULL;
		if( CreateNewContext( context_struct ) != 0 )
			std::cout<< "ContextManager - Error. Can't allocate new context structure. Out of memory?\n";

		if (context_struct == NULL)
			printf("Error, CreateNewContext failed to return a valid context_struct\n");
		
		context_struct->list_id = list_id;
		if (free_list != NULL)
			free_list->freeStack.push_back( context_struct );
		else 
			printf("Error, free_list is null and cannot insert the context_struct\n");
	}

	LeaveCriticalSection( &free_list->crit_section );

	return list_id;
}

// Makes sure that a few conditions are satisfied before granting the FreeList to the
//  calling thread
BOOL ContextManager::TryEnterAndCheck( const int& list_id )
{
	//FreeList* free_list = m_listOfFreeLists[list_id];
	FreeList* free_list = getIndexElement( list_id );

	// If another thread is currently holding the Critical Section, return false
	if( TryEnterCriticalSection(&(free_list->crit_section)) == FALSE )
	{
		EnterCriticalSection( &m_debug_criticalSection );

		m_debug_totalNumEnterTries++;

		LeaveCriticalSection( &m_debug_criticalSection );
		return FALSE;
	}

	// If held FreeList has available ContextStructures, return true
	if( !free_list->freeStack.empty() )	
		return TRUE;
	else
	{
		EnterCriticalSection( &m_debug_criticalSection );

		m_debug_totalNumTimesNoElementsFree++;

		LeaveCriticalSection( &m_debug_criticalSection );		
	}

	// If held FreeList has no free ContextStructures but has enough room to add more, return true
	if( free_list->length < m_max_list_length )
		return TRUE;
	else
	{
		EnterCriticalSection( &m_debug_criticalSection );

		m_debug_totalNumTimesListFull++;

		LeaveCriticalSection( &m_debug_criticalSection );
	}

	// At this point, managed to enter the critical section but failed the other checks
	// Leave the critical section and return failure
	LeaveCriticalSection( &(free_list->crit_section) );
	return FALSE;
}


int ContextManager::GetFreeContext( ContextStructure* &free_struct )
{
	int start_hash_pos = rand() % m_numFreeLists;
	int list_id = start_hash_pos;
	BOOL obtainedCritSection = FALSE;
	FreeList* free_list;
	//ContextStructure* context_struct;

	//Debug counters
	int num_tries = 0;

	// Try to obtain a critical section for one of the lists
	obtainedCritSection = TryEnterAndCheck(list_id);
	num_tries++;

	while( obtainedCritSection == FALSE )
	{
		list_id++;

		if( list_id >= m_numFreeLists )
		{
			list_id = 0;
		}
		
		if( list_id == start_hash_pos )
			break;

		obtainedCritSection = TryEnterAndCheck(list_id);
		num_tries++;
	}

	// Went through the whole vector and didn't find a free one, so Create a new FreeList
	if( obtainedCritSection == FALSE )
	{
		// TODO: What if it doesn't get to enter right away and free structures get used up
		list_id = CreateNewFreeList();
		//free_list = m_listOfFreeLists[list_id];
		free_list = getIndexElement( list_id );
		EnterCriticalSection(&(free_list->crit_section));
	}
	else
	{
		//free_list = m_listOfFreeLists[list_id];
		free_list = getIndexElement( list_id );
	}

	// Either obtain a free ContextStructure or create a new one to be added on Release.
	if( free_list->freeStack.empty() )
	{
		CreateNewContext( free_struct );
		free_struct->list_id = list_id;

		free_list->length++;		
	}
	else
	{
		// Treating the freeStack as a stack (LIFO) 
		free_struct = free_list->freeStack.back();
		free_list->freeStack.pop_back();
	}

	LeaveCriticalSection( &(free_list->crit_section) );

	EnterCriticalSection( &m_debug_criticalSection );

	m_debug_numAccesses++;
	m_debug_totalNumTries += num_tries;

	LeaveCriticalSection( &m_debug_criticalSection );

	return 0;
}

// Push the ContextStructure back on to the stack of free ContextStructures that it came from
int ContextManager::ReleaseContext( ContextStructure* free_struct )
{
	//FreeList* free_list = m_listOfFreeLists[free_struct->list_id];
	FreeList* free_list = getIndexElement( free_struct->list_id );

	EnterCriticalSection( &free_list->crit_section );

	free_list->freeStack.push_back( free_struct );

	LeaveCriticalSection( &free_list->crit_section );

	return 0;
}

int ContextManager::ResizeMemory( char *&mem_ptr, const int &target_size )
{
	try
	{
		//delete old memory block
		if( mem_ptr != NULL )
			delete [] mem_ptr;

		//allocate new memory block
		mem_ptr = new char[target_size];
		//set new size
		///curr_size = target_size;
	}
	catch(std::exception e)
	{
		return -1;
	}
	
	return 0;
}

int ContextManager::ReallocMemory( char *&mem_ptr, const int& curr_size, const int &target_size )
{
	try
	{
		//delete old memory block
		//delete [] mem_ptr;
		//allocate new memory block
		//mem_ptr = new char[target_size];
		//set new size
		///curr_size = target_size;

		char* new_memory = new char[target_size];
		if( curr_size < target_size )
			memcpy( new_memory, mem_ptr, curr_size );
		else
			memcpy( new_memory, mem_ptr, target_size );

		mem_ptr = new_memory;
		
	}
	catch(std::exception e)
	{
		return -1;
	}
	
	return 0;
}

// Attempts multiple times to lookup the STL vector at the specified array
//  index. Sometimes the array dereference gives a bad pointer, due to the
//  structure being modified by some other thread. To avoid blocking threads
//  to handle the modifications, this function retries the get until it
//  finds a valid pointer.
FreeList* ContextManager::getIndexElement( const int& list_id )
{
	FreeList* freeList;
	bool obtainedValid = false;

	while( obtainedValid == false )
	{
		__try
		{
			freeList = m_listOfFreeLists[list_id];

			// Try to make a call using the pointer
			freeList->freeStack.empty();

			obtainedValid = true;
		} 
		__except( EXCEPTION_EXECUTE_HANDLER )
		{
			obtainedValid = false;
			Sleep(0);
		}
	}

	return freeList;
}


void ContextManager::printStats()
{
	std::cout<< "Context Manager Statistics:\n";
	std::cout<< "# total accesses: " << m_debug_numAccesses << std::endl
		<< "# total calls to \'new\': " << m_debug_totalNumNew << std::endl
		<< "Avg. # tries per access: " << m_debug_totalNumTries / m_debug_numAccesses << std::endl
		<< "Avg. # EnterCriticalSection retries: " << m_debug_totalNumEnterTries / m_debug_numAccesses << std::endl
		<< "Avg. # times all elements in list were unavailable: " << m_debug_totalNumTimesNoElementsFree / m_debug_numAccesses << std::endl
		<< "Avg. # times unavailable AND max elements reached: " << m_debug_totalNumTimesListFull / m_debug_numAccesses << std::endl;
}