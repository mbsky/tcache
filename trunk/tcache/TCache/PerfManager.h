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

#ifndef PERF_MANAGER_H__
#define PERF_MANAGER_H__

#include "PerfCounters.h"


/**
 * PerfManager
 * 
 * NOT THREAD SAFE - use only with a single thread or with synchronized access using multiple threads
 */
class PerfManager
{
	// PerfMon counters
private:
	static MyPerfMonManager m_perfMgr;
	static MyPerfObjects **m_pPerfObjInstArray;
	static int	m_numPerfObjInstances;
	
	
	int InitPerformanceCounters();
	int ReleasePerformanceCounters();

	// Callbacks
public:
	static void __stdcall IncrementCounter ( const long& counterID, const long& perfObjInstance );
	static void __stdcall DecrementCounter ( const long& counterID, const long& perfObjInstance );
	static void __stdcall IncreaseCounter ( const long& counterID, const long& value, const long& perfObjInstance );
	static void __stdcall IncreaseLargeCounter ( const long& counterID, const long long& value, const long& perfObjInstance );
	static void __stdcall SetCounter( const long& counterID, const long& value, const long& perfObjInstance );
	static void __stdcall SetLargeCounter( const long& counterID, const long long& value, const long& perfObjInstance );

public:
	// Specify negative for value to subtract
	static void __stdcall AddNumCacheHits( const long& value, const long& perfObjInstance );
	static void __stdcall AddNumRequests( const long& value, const long& perfObjInstance );
	static void __stdcall AddNumBytesAccessedTotal( const long long& value, const long& perfObjInstance );
	static void __stdcall AddNumBytesCacheHit( const long long& value, const long& perfObjInstance );
	static void __stdcall AddNumEvictions( const long& value, const long& perfObjInstance );
	static void __stdcall AddNumGets( const long& value, const long& perfObjInstance );
	static void __stdcall AddNumInserts( const long& value, const long& perfObjInstance );
	static void __stdcall AddNumDeletes( const long& value, const long& perfObjInstance );
	



	// Counter IDs
public:
	const static long	NumQ = 1;
	const static long	NumRequests = 2;
	const static long	NumCacheHits = 3;
	const static long	NumBytesAccessedTotal = 4;
	const static long	NumBytesCacheHit = 5;
	const static long	NumEvictions = 6;
	const static long	NumGets = 7;
	const static long	NumInserts = 8;
	const static long	NumDeletes = 9;
	const static long	CleanBytes = 10;
	const static long	DirtyBytes = 11;
	const static long	TotalBytes = 12;	
	const static long	NumFreeSpaceBytes = 13;
	const static long	AsynchronousMode = 14;
	const static long	NumVictimizeFailures = 15;
	const static long	NumDeadlocks = 16;
	const static long	NumRequestsKeyFound = 17;
	

	const static long	NumRequestsPerSecond = 1002;
	const static long	NumCacheHitsPerSecond = 1003;
	const static long	NumBytesAccessedTotalPerSecond = 1004;
	const static long	NumBytesCacheHitPerSecond = 1005;
	const static long	NumEvictionsPerSecond = 1006;
	const static long	NumGetsPerSecond = 1007;
	const static long	NumInsertsPerSecond = 1008;
	const static long	NumDeletesPerSecond = 1009;
	const static long	NumRequestsKeyFoundPerSecond = 1017;

	const static long	WindowNumRequests = 5002;
	const static long	WindowNumCacheHits = 5003;
	const static long	WindowNumBytesAccessedTotal = 5004;
	const static long	WindowNumBytesCacheHit = 5005;
	const static long	WindowNumEvictions = 5006;
	const static long	WindowNumRequestsKeyFound = 5017;

public:
	int Initialize( int numPerfObjInstances );
	int	Shutdown();

};

#endif // PERF_MANAGER_H__
