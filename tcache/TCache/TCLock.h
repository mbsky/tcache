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

#ifndef __TC_LOCK_H_
#define __TC_LOCK_H_

#include <Windows.h>
#include <iostream>
#include <string>
#include <sstream>
#include "Vdt.h"
#include "Common.h"

#define TEMP_NUM_HASH_VALUES 1000


class _export TCLock
{
private:
	CRITICAL_SECTION *m_lockCritSection;
	int m_numCritSections;	
	int m_hashArraySize;
	char *m_hashArray;
	//char m_hashArray[TEMP_NUM_HASH_VALUES];	
	bool m_useBernsteinHash;

	// Debug counters
	double m_totalNumLoops;
	int m_numCollisionsBits;
	int m_numCollisionsCritSections;
	int m_numAccesses;	
	int m_totalWaitAccesses;

	// Hash functions
	int hash( const Vdt &dzName, const Vdt &key );
	int bernsteinHash (const Vdt &dzname, const Vdt &key);   
    int sumHash (const Vdt &dzname, const Vdt &key);

	int hashCriticalSection( const int& hash_pos );

	// Array bit manipulation functions
	bool bitGet( const int& hash_pos );
	int bitSet( const int& hash_pos );
	int bitClear( const int& hash_pos );

public:
	TCLock( const int& num_hash_values, const int& num_threads );
	~TCLock();

	// Blocks thread to wait until it acquires the lock
	int acquireWait( const Vdt &dzName, const Vdt &key );

	// Returns failure if it does not succeed in acquiring the lock in 1 try
	int acquireFail( const Vdt &dzName, const Vdt &key );

	// Releases an acquired lock. Returns an error if release is called when the lock has
	//  not been acquired
	int release( const Vdt &dzName, const Vdt &key );

	// Prints to console statistics regarding the lock functions. Only has useful
	//  information if TC_LOCK_DEBUG is set to 1
	void printStats();
};

#endif // __TC_LOCK_H_