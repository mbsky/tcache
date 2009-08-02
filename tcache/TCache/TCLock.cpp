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

// TCLock.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "TCLock.h"
#include <windows.h>


int TCLock::hash( const Vdt &dzName, const Vdt &key )
{
	if( m_useBernsteinHash )
		return bernsteinHash( dzName, key );
	else
		return sumHash( dzName, key );
}


int TCLock::hashCriticalSection( const int& hash_pos )
{
	return (hash_pos / 8) % m_numCritSections;
}


bool TCLock::bitGet( const int& hash_pos )
{
	int array_index = hash_pos / 8;
	int bit_index = hash_pos % 8;

	// Right shift 1000 0000 to the appropriate bitmask ( ex: idx=3, mask=0001 0000 )
	char bitmask = 0x80 >> bit_index;

	if( m_hashArray[array_index] & bitmask )
		return true;
	else
		return false;
}


int TCLock::bitSet( const int& hash_pos )
{
	int array_index = hash_pos / 8;
	int bit_index = hash_pos % 8;

	// Right shift 1000 0000 to the appropriate bitmask ( ex: idx=3, mask=0001 0000 )
	char bitmask = 0x80 >> bit_index;

	m_hashArray[array_index] = m_hashArray[array_index] | bitmask;

	return 0;
}


int TCLock::bitClear( const int& hash_pos )
{
	int array_index = hash_pos / 8;
	int bit_index = hash_pos % 8;

	// Right shift 1000 0000 to the appropriate bitmask ( ex: idx=3, mask=0001 0000 )
	char bitmask = 0x80 >> bit_index;

	// Invert the bitmask ( ex: idx=3, mask=1110 1111 )
	bitmask = ~bitmask;

	m_hashArray[array_index] = m_hashArray[array_index] & bitmask;

	return 0;
}


int TCLock::bernsteinHash (const Vdt &dzname, const Vdt &key)
{
    char *Dzname= (char *) dzname.get_data();
    char *Key = (char*) key.get_data();

    int retHash=5381;
    int len=0;

    while(len<dzname.get_size())
    {
        retHash = ((retHash << 5) + retHash) + *Dzname++;
        len++;
    }

    len=0;
    while(len<key.get_size())
    {
        retHash = ((retHash << 5) + retHash) + *Key++;
        len++;
    }   
   
    if (retHash < 0) retHash = -retHash;
    return retHash%m_hashArraySize;
}


int TCLock::sumHash (const Vdt &dzname, const Vdt &key)
{
    char *Dzname= (char *) dzname.get_data();
    char *Key = (char*) key.get_data();
   
    int retHash=0;
    int len=0;
    while(len<dzname.get_size())
    {
        retHash+=*Dzname++;
        len++;
    }
    len=0;
    while(len<key.get_size())
    {
        retHash+=*Key++;
        len++;
    }
    if (retHash < 0) retHash = -retHash;
    return retHash%m_hashArraySize;
}


TCLock::TCLock( const int& num_hash_values, const int& num_threads ) 
{ 
	// Size of array in bits
	m_hashArraySize = num_hash_values * 8;
	m_hashArray = new char[num_hash_values];

	// Use BernsteinHash or SumHash
	m_useBernsteinHash = true;

	// Clear the array first
	memset( m_hashArray, 0, m_hashArraySize / 8 * sizeof(char) );

	// 1 extra critical section for internal statistics
	m_numCritSections = num_threads;
	m_lockCritSection = new CRITICAL_SECTION[num_threads + 1];
	for( int i = 0; i < num_threads + 1; i++ )
	{
		InitializeCriticalSection( &m_lockCritSection[i] );
	}

	// Initialize statistic counters
	EnterCriticalSection( &m_lockCritSection[m_numCritSections] );
	
	m_numAccesses = 0;
	m_numCollisionsBits = 0;
	m_numCollisionsCritSections = 0;
	m_totalNumLoops = 0;
	m_totalWaitAccesses = 0;

	LeaveCriticalSection( &m_lockCritSection[m_numCritSections] );
}

TCLock::~TCLock()
{
	// Error checking. Make sure that no locks are still acquired
	int num_locks_active = 0;
	for( int i = 0; i < m_hashArraySize / 8; i++ )
	{
		if( m_hashArray[i] != 0 )
			num_locks_active++;
	}

	if( num_locks_active > 0 )
		std::cout<< "Warning. At least " << num_locks_active << " locks are still active in the system. "
				<< "Some locks were not released before shutdown.\n";

	if( m_hashArray )
	{
		delete [] m_hashArray;
		m_hashArray = NULL;
	}

	if( m_lockCritSection )
	{
		delete [] m_lockCritSection;
		m_lockCritSection = NULL;
	}
}

int TCLock::acquireWait( const Vdt &dzName, const Vdt &key )
{
	int hash_pos = hash( dzName, key );
	int crit_section = hashCriticalSection( hash_pos );
	bool lock_acquired = false;
	
#if TC_LOCK_DEBUG
	// Debug counters
	int num_loops = 1;
	bool bitCollision = false;
	bool criticalSectionCollision = false;


	while( !lock_acquired )
	{
		if( !TryEnterCriticalSection( &m_lockCritSection[crit_section] ) )
		{		
			criticalSectionCollision = true;
			EnterCriticalSection( &m_lockCritSection[crit_section] );
		}
		if( !bitGet(hash_pos) )
		{
			//m_hashArray[hash_pos]++;
			bitSet(hash_pos);
			lock_acquired = true;
		}
		LeaveCriticalSection( &m_lockCritSection[crit_section] );			

		if( !lock_acquired )
		{
			// Context switch
			Sleep(0);
			num_loops++;

			bitCollision = true;
		}
	}

	EnterCriticalSection( &m_lockCritSection[m_numCritSections] );

	m_numAccesses++;

	if( bitCollision )
		m_numCollisionsBits++;

	if( criticalSectionCollision )
		m_numCollisionsCritSections++;

	m_totalNumLoops += num_loops;

	m_totalWaitAccesses++;

	LeaveCriticalSection( &m_lockCritSection[m_numCritSections] );
	
#else
	while( !lock_acquired )
	{
		EnterCriticalSection( &m_lockCritSection[crit_section] );
		if( !bitGet(hash_pos) )
		{
			//m_hashArray[hash_pos]++;
			bitSet(hash_pos);
			lock_acquired = true;
		}
		LeaveCriticalSection( &m_lockCritSection[crit_section] );			

		if( !lock_acquired )
		{
			// Context switch
			Sleep(0);
		}
	}
#endif

	return 0;
}

int TCLock::acquireFail( const Vdt &dzName, const Vdt &key )
{
	int hash_pos = hash( dzName, key );
	int crit_section = hashCriticalSection( hash_pos );
	bool lock_acquired = false;
	bool criticalSectionCollision = false;

#if TC_LOCK_DEBUG
	// Debug counters
	int num_loops = 0;
	bool alreadyCounted = false;

	if( !TryEnterCriticalSection( &m_lockCritSection[crit_section] ) )
	{
		criticalSectionCollision = true;	
		EnterCriticalSection( &m_lockCritSection[crit_section] );
	}
	if( !bitGet(hash_pos) )
	{
		bitSet(hash_pos);
		lock_acquired = true;
	}
	LeaveCriticalSection( &m_lockCritSection[crit_section] );			



	EnterCriticalSection( &m_lockCritSection[m_numCritSections] );

	m_numAccesses++;

	if( !lock_acquired )
	{
		m_numCollisionsBits++;
	}

	if( criticalSectionCollision )
		m_numCollisionsCritSections++;

	LeaveCriticalSection( &m_lockCritSection[m_numCritSections] );
	
#else

	EnterCriticalSection( &m_lockCritSection[crit_section] );
	if( !bitGet(hash_pos) )
	{
		//m_hashArray[hash_pos]++;
		bitSet(hash_pos);
		lock_acquired = true;
	}
	LeaveCriticalSection( &m_lockCritSection[crit_section] );
#endif

	if( lock_acquired )
		return 0;
	else
		return -1;
}

int TCLock::release( const Vdt &dzName, const Vdt &key )
{
	int hash_pos = hash( dzName, key );
	int crit_section = hashCriticalSection( hash_pos );
	bool lock_released = false;

	EnterCriticalSection( &m_lockCritSection[crit_section] );
	if( bitGet(hash_pos) )
	{
		bitClear(hash_pos);
		lock_released = true;
	}
	else
	{
		std::cout<< "Error: unexpected lock value at hash location. Got a " 
			<< "0 but was expecting a 1. Check that you are releasing a lock you actually acquired\n";
	}
	LeaveCriticalSection( &m_lockCritSection[crit_section] );

	if( lock_released )
		return 0;
	else
		return -1;
}

void TCLock::printStats()
{
	std::cout<< "Lock statistics\n";
	std::cout<< "Total # of accesses: " << m_numAccesses << std::endl
		<< "# of collisions(bits): " << m_numCollisionsBits << std::endl
		<< "# of collisions(critical sections): " << m_numCollisionsCritSections << std::endl
		<< "Avg. # of loops per acquireWait: " << m_totalNumLoops / m_totalWaitAccesses << std::endl;
}
