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

#ifndef __STATISTICS_
#define __STATISTICS_

#include <Windows.h>
#include <iostream>
#include "TCLock.h"
#include "PerfCounters.h"
#include "PerfManager.h"
#include "SlidingWindowData.h"


typedef void (__stdcall *CallbackSig_int32_int32)( const long&, const long& );
typedef void (__stdcall *CallbackSig_int32_int32_int32)( const long&, const long&, const long& );
typedef void (__stdcall *CallbackSig_int32_int64_int32)( const long&, const long long&, const long& );


struct stats
{
	double	totalQ;
	double	L;
	long long cleanBytes;
	long long dirtyBytes;
	long long freespaceBytes;
	long	lastEvictedAgeValue;
	long	numQ;	
	bool	asynchronous; // set to true if asynchronus or set to false if it is synchronous
	bool	resetFreespace;

	__int64	NumBytesAccessedTotal;
	__int64	NumBytesCacheHit;
	long	NumRequests;
	long	NumCacheHits;
	long	NumRequestsKeyFound;	
	long	NumEvictions;
	long	NumGets;
	long	NumInserts;
	long	NumDeletes;
	long	NumVictimizeFailures;
	long	NumDeadlocks;

	SlidingWindowData<__int64>* WindowBytesAccessedTotal;
	SlidingWindowData<__int64>* WindowBytesCacheHit;
	SlidingWindowData<long>*	WindowNumRequests;
	SlidingWindowData<long>*	WindowNumCacheHits;
	SlidingWindowData<long>*	WindowNumEvictions;

};


class _export StatisticsManager
{
private:
	int m_numOfPartition;
	stats *m_statistics;
	TCLock *m_lock;
	long m_numSynchronousInsert;
	bool m_perfManagerInitialized;

	
	void PartitionIDtoVdt (short &partitionID, Vdt &dzname, Vdt &key) const;

	// PerfMon counters
private:
	//int InitPerformanceCounters();
	//int ReleasePerformanceCounters();
	//MyPerfMonManager _perfMgr;
	//MyPerfObjects *_pPerfObjInst1;
	PerfManager m_perfManager;


	void SetCallbackIncrementCounter( CallbackSig_int32_int32 cbFunction ) { m_incrementCounter = cbFunction; }
	void SetCallbackDecrementCounter( CallbackSig_int32_int32 cbFunction ) { m_decrementCounter = cbFunction; }
	void SetCallbackIncreaseCounter( CallbackSig_int32_int32_int32 cbFunction ) { m_increaseCounter = cbFunction; }
	void SetCallbackIncreaseLargeCounter( CallbackSig_int32_int64_int32 cbFunction ) { m_increaseLargeCounter = cbFunction; }
	void SetCallbackSetCounter( CallbackSig_int32_int32_int32 cbFunction ) { m_setCounter = cbFunction; }
	void SetCallbackSetLargeCounter( CallbackSig_int32_int64_int32 cbFunction ) { m_setLargeCounter = cbFunction; }

	CallbackSig_int32_int32 m_incrementCounter;
	CallbackSig_int32_int32 m_decrementCounter;
	CallbackSig_int32_int32_int32	m_increaseCounter;
	CallbackSig_int32_int64_int32	m_increaseLargeCounter;
	CallbackSig_int32_int32_int32	m_setCounter;
	CallbackSig_int32_int64_int32	m_setLargeCounter;

public:
	StatisticsManager(short numOfPartition,TCLock *lock);
	~StatisticsManager();

	int IncreaseTotalQ(short partitionID,double val);
	int DecreaseTotalQ(short partitionID,double val);
	int IncrementNumQ(short partitionID);
	int DecrementNumQ (short partitionID);
	
	double GetAvgQ (short partitionID) const;
	
	int SetL (short partitionID,double minQ);
	double GetL (short partitionID) const; 

	int SetLastEvictedAgeValue (short partitionID,long lastEvictedAgeValue);
	int GetLastEvictedAgeValue (short partitionID) const;
	
	int IncreaseCleanBytes(short partitionID, long long bytes);
	int DecreaseCleanBytes(short partitionID, long long bytes);
	int IncreaseDirtyBytes(short partitionID, long long bytes);
	int DecreaseDirtyBytes(short partitionID, long long bytes);
	
	long long GetCleanBytes(short partitionID) const;
	long long GetDirtyBytes(short partitionID) const;
	

	bool GetPartitionMode (short partitionID) const;
	int  SetPartitionMode (short partitionID,bool mode); // mode to true if asynchronus or set to false if it is synchronous

	int IncreaseFreeSpaceBytes(short partitionID, long long bytes);
	int SetFreeSpaceBytes(short partitionID, long long bytes);
	long long GetFreeSpaceBytes(short partitionID) const;

	int IncreaseNumBytesCacheHit(short partitionID, long long bytes);
	int IncreaseNumBytesAccessedTotal(short partitionID, long long bytes);
	int IncrementNumRequests(short partitionID);
	int IncrementNumCacheHits(short partitionID);
	int IncrementNumGets(short partitionID);
	int IncrementNumInserts(short partitionID);
	int IncrementNumDeletes(short partitionID);
	int IncreaseNumEvictions(short partitionID, long numEvictions);
	int IncreaseNumVicimizeFailures(short partitionID, long numFailures);
	int IncreaseNumDeadlocks(short partitionID, long numDeadlocks);
	int IncrementNumRequestsKeyFound(short partitionID);

	double GetCacheHitRate(short partitionID=-1) const; 
	double GetByteHitRate(short partitionID=-1) const; 
	int GetNumQ(short partitionID=-1) const;
	long long GetTotalBytes() const;

	long GetNumEvictions(short partitionID=-1) const;
	long GetNumVictimizeFailures (short partitionID=-1) const;
	long GetNumDeadlocks (short partitionID=-1) const;
	

	// val is either 1 or -1 signifying increment or decrement respectively
	int ModifyNumSynchronousInsert(short val);
	long GetNumSynchronousInsert () const;

	// This functions only resets the statistics relating to Performance Counters
	// Things like clean bytes/dirty bytes are not reset as they are needed by the system
	// if partitionID < 0, all partitions are reset
	int ResetPerfCounterStatistics(short partitionID);

	double GetTotalCacheHitRate (short partitionID=-1) const;
	double GetTotalByteHitRate (short partitionID=-1) const;
};


#endif // __Statistics
