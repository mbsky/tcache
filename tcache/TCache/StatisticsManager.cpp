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

// StatisticsManager.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "StatisticsManager.h"

#define ACQUIRE_WAIT Vdt dzname,key;\
					 PartitionIDtoVdt (partitionID, dzname, key);\
					 m_lock->acquireWait(dzname,key);

#define RELEASE m_lock->release(dzname,key);

#define SANITY_CHECK(num,field) if(num<0) printf("StatisticsManager::Error %s is less than 0\n",field)

#define PARTITION_ID_VALIDITY_TEST if(partitionID<0 || partitionID>=m_numOfPartition){\
									printf("StatisticsManager::Error Partition ID out of range\n");\
									return PARTITION_INVALID;};

StatisticsManager::StatisticsManager(short numOfPartition,TCLock *lock)
{
	m_statistics= new stats [numOfPartition];
	m_numOfPartition=numOfPartition;
	for(int i=0;i<m_numOfPartition;i++)
	{
		m_statistics[i].cleanBytes=0;
		m_statistics[i].dirtyBytes=0;
		m_statistics[i].L=0;
		m_statistics[i].numQ=0;
		m_statistics[i].totalQ=0;
		m_statistics[i].asynchronous=true;
		m_statistics[i].freespaceBytes = 0;
		m_statistics[i].lastEvictedAgeValue = 0;

		m_statistics[i].NumCacheHits = 0;
		m_statistics[i].NumRequests = 0;
		m_statistics[i].NumRequestsKeyFound = 0;

		m_statistics[i].NumEvictions = 0;

		m_statistics[i].NumGets = 0;
		m_statistics[i].NumDeletes = 0;
		m_statistics[i].NumInserts = 0;
		m_statistics[i].NumVictimizeFailures = 0;
		m_statistics[i].NumBytesAccessedTotal = 0;
		m_statistics[i].NumBytesCacheHit = 0;
		m_statistics[i].NumDeadlocks = 0;

		m_statistics[i].WindowBytesAccessedTotal = new SlidingWindowData<__int64>(12,5);
		m_statistics[i].WindowBytesCacheHit = new SlidingWindowData<__int64>(12,5);
		m_statistics[i].WindowNumRequests = new SlidingWindowData<long>(12,5);
		m_statistics[i].WindowNumCacheHits = new SlidingWindowData<long>(12,5);
		m_statistics[i].WindowNumEvictions = new SlidingWindowData<long>(12,5);
		
	}
	m_lock=lock;
	m_numSynchronousInsert=0;
	
	m_perfManagerInitialized = false;
	int ret = m_perfManager.Initialize( numOfPartition );

	m_incrementCounter = NULL;
	m_decrementCounter = NULL;
	m_increaseLargeCounter = NULL;
	m_increaseCounter = NULL;
	m_setCounter = NULL;
	m_setLargeCounter = NULL;

	// Only enable set functions if PerfCounters could be intialized
	if( ret == 0 )
	{
		m_perfManagerInitialized = true;
		SetCallbackIncreaseLargeCounter( m_perfManager.IncreaseLargeCounter );
		SetCallbackSetCounter( m_perfManager.SetCounter );
		SetCallbackSetLargeCounter( m_perfManager.SetLargeCounter );
	}	
}

void StatisticsManager::PartitionIDtoVdt (short &partitionID, Vdt &dzname, Vdt &key) const
{
	
	dzname.set_data((char*)&partitionID);
	dzname.set_size(sizeof(short));
	key.set_data(NULL);
	key.set_size(0);
}

StatisticsManager::~StatisticsManager()
{
	if( m_perfManagerInitialized )
		m_perfManager.Shutdown();

	for( int i = 0; i < m_numOfPartition; i++ )
	{
		delete m_statistics[i].WindowBytesAccessedTotal;
		delete m_statistics[i].WindowBytesCacheHit;
		delete m_statistics[i].WindowNumRequests;
		delete m_statistics[i].WindowNumCacheHits;
		delete m_statistics[i].WindowNumEvictions;
	}

	delete[] m_statistics;
}
int StatisticsManager::IncreaseTotalQ(short partitionID,double val)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].totalQ+=val;
	RELEASE 
	return 0;
}

int StatisticsManager::DecreaseTotalQ(short partitionID,double val)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].totalQ-=val;
	RELEASE 
	SANITY_CHECK(m_statistics[partitionID].totalQ,"TotalQ");
	return 0;
}

int StatisticsManager::IncrementNumQ(short partitionID)
{
	PARTITION_ID_VALIDITY_TEST
	InterlockedIncrement(&m_statistics[partitionID].numQ);
	if( m_setCounter )
		m_setCounter( PerfManager::NumQ, m_statistics[partitionID].numQ, partitionID );
	return 0;
}

int StatisticsManager::DecrementNumQ (short partitionID)
{
	PARTITION_ID_VALIDITY_TEST
	InterlockedDecrement(&m_statistics[partitionID].numQ);
	if( m_setCounter )
		m_setCounter( PerfManager::NumQ, m_statistics[partitionID].numQ, partitionID );
	SANITY_CHECK(m_statistics[partitionID].numQ,"NumQ");
	return 0;
}

double StatisticsManager::GetAvgQ (short partitionID) const
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT

	// Use a default of the totalQ so that if the next check fails,
	//  the avg returned will be high, forcing the system to update
	//  the metadata
	double avg = m_statistics[partitionID].totalQ;

	// Avoid division by zero
	if( m_statistics[partitionID].numQ > 0 )
		avg /= m_statistics[partitionID].numQ;

	RELEASE 
	SANITY_CHECK(avg,"AvgQ");
	return avg;
	
}

int StatisticsManager::SetL (short partitionID,double minQ)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].L=minQ;
	RELEASE 
	SANITY_CHECK(minQ,"L");
	return 0;
}
double StatisticsManager::GetL (short partitionID) const
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	double L=m_statistics[partitionID].L;
	RELEASE 
	return L;
}

int StatisticsManager::SetLastEvictedAgeValue (short partitionID,long lastEvictedAgeValue)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].lastEvictedAgeValue = lastEvictedAgeValue;
	RELEASE 
	return 0;
}

int StatisticsManager::GetLastEvictedAgeValue (short partitionID) const
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	int lastEvictedAgeValue = m_statistics[partitionID].lastEvictedAgeValue;
	RELEASE 
	return lastEvictedAgeValue;
}

int StatisticsManager::IncreaseCleanBytes(short partitionID, long long bytes)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].cleanBytes+=bytes;

	if( m_increaseLargeCounter )
	{
		m_increaseLargeCounter( PerfManager::CleanBytes, bytes, partitionID );
		m_increaseLargeCounter( PerfManager::TotalBytes, bytes, partitionID );
	}

	RELEASE 
	return 0;
}

int StatisticsManager::DecreaseCleanBytes(short partitionID, long long bytes)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].cleanBytes-=bytes;
	RELEASE 
	SANITY_CHECK(m_statistics[partitionID].cleanBytes,"CleanBytes");	
	return 0;
}

int StatisticsManager::IncreaseDirtyBytes(short partitionID, long long bytes)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].dirtyBytes+=bytes;

	if( m_increaseLargeCounter )
	{
		m_increaseLargeCounter( PerfManager::DirtyBytes, bytes, partitionID );
		m_increaseLargeCounter( PerfManager::TotalBytes, bytes, partitionID );
	}
	RELEASE
	if (m_statistics[partitionID].dirtyBytes<0)
		printf("Ditry bytes less than 0\n");
	return 0;
}
int StatisticsManager::DecreaseDirtyBytes(short partitionID, long long bytes)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].dirtyBytes-=bytes;
	RELEASE 
	SANITY_CHECK(m_statistics[partitionID].cleanBytes,"DirtyBytes");	
	return 0;
}

long long StatisticsManager::GetCleanBytes(short partitionID) const
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	long long cleanBytes=m_statistics[partitionID].cleanBytes;
	RELEASE 
	return cleanBytes;
}
long long StatisticsManager::GetDirtyBytes(short partitionID) const
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	long long dirtyBytes=m_statistics[partitionID].dirtyBytes;
	RELEASE 
	return dirtyBytes;
}

bool StatisticsManager::GetPartitionMode (short partitionID) const
{
	//PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	bool mode=m_statistics[partitionID].asynchronous;
	RELEASE 
	return mode;
}
int StatisticsManager::SetPartitionMode (short partitionID,bool mode)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].asynchronous=mode;
	if( m_setCounter )
	{
		long mode_num = 0;
		if( mode )
			mode_num = 1;
		m_setCounter( PerfManager::AsynchronousMode, mode_num, partitionID );
	}
	RELEASE 
	return 0;
}

long long StatisticsManager::GetFreeSpaceBytes(short partitionID) const
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	long long freeBytes=m_statistics[partitionID].freespaceBytes;
	RELEASE 
	return freeBytes;
}

int StatisticsManager::IncreaseFreeSpaceBytes(short partitionID, long long bytes)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].freespaceBytes += bytes;
	if( m_statistics[partitionID].freespaceBytes < 0 )
		m_statistics[partitionID].freespaceBytes = 0;

	if( m_setLargeCounter )
	{
		m_setLargeCounter( PerfManager::NumFreeSpaceBytes, m_statistics[partitionID].freespaceBytes, partitionID );
	}

	RELEASE 
	return 0;
}

int StatisticsManager::SetFreeSpaceBytes(short partitionID, long long bytes)
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT	
	if( bytes < 0 )
		m_statistics[partitionID].freespaceBytes = 0;
	else
		m_statistics[partitionID].freespaceBytes = bytes;

	if( m_setLargeCounter )
	{
		m_setLargeCounter( PerfManager::NumFreeSpaceBytes, m_statistics[partitionID].freespaceBytes, partitionID );
	}

	RELEASE 
	return 0;
}



int StatisticsManager::IncrementNumCacheHits( short partitionID )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumCacheHits++;
	//_pPerfObjInst1->NumCacheHits = m_statistics[partitionID].NumCacheHits;
	//_pPerfObjInst1->NumCacheHitsPerSecond++;
	m_statistics[partitionID].WindowNumCacheHits->addValue( 1 );

	if( m_setCounter )
	{
		m_setCounter( PerfManager::NumCacheHits, m_statistics[partitionID].NumCacheHits, partitionID ); 
		m_setCounter( PerfManager::WindowNumCacheHits, m_statistics[partitionID].WindowNumCacheHits->getTotalValue(), partitionID );
	}
	
	RELEASE
	return 0;
}

int StatisticsManager::IncrementNumRequests( short partitionID )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumRequests++;
	m_statistics[partitionID].WindowNumRequests->addValue( 1 );

	if( m_setCounter )
	{
		m_setCounter( PerfManager::NumRequests, m_statistics[partitionID].NumRequests, partitionID ); 
		m_setCounter( PerfManager::WindowNumRequests, m_statistics[partitionID].WindowNumRequests->getTotalValue(), partitionID );
	}

	RELEASE
	return 0;
}

int StatisticsManager::IncrementNumRequestsKeyFound( short partitionID )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumRequestsKeyFound++;

	if( m_setCounter )
	{
		m_setCounter( PerfManager::NumRequestsKeyFound, m_statistics[partitionID].NumRequestsKeyFound, partitionID ); 		
	}

	RELEASE
	return 0;
}

int StatisticsManager::IncreaseNumBytesAccessedTotal( short partitionID, long long bytes )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumBytesAccessedTotal += bytes;
	m_statistics[partitionID].WindowBytesAccessedTotal->addValue( bytes );
	if( m_setLargeCounter )
		m_setLargeCounter( PerfManager::NumBytesAccessedTotal, m_statistics[partitionID].NumBytesAccessedTotal, partitionID );
	if( m_setLargeCounter )
		m_setLargeCounter( PerfManager::WindowNumBytesAccessedTotal, m_statistics[partitionID].WindowBytesAccessedTotal->getTotalValue(), partitionID );
	RELEASE
	return 0;
}

int StatisticsManager::IncreaseNumBytesCacheHit( short partitionID, long long bytes )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumBytesCacheHit += bytes;
	m_statistics[partitionID].WindowBytesCacheHit->addValue( bytes );
	if( m_setLargeCounter )
		m_setLargeCounter( PerfManager::NumBytesCacheHit, m_statistics[partitionID].NumBytesCacheHit, partitionID );
	if( m_setLargeCounter )
		m_setLargeCounter( PerfManager::WindowNumBytesCacheHit, m_statistics[partitionID].WindowBytesCacheHit->getTotalValue(), partitionID );
	RELEASE
	return 0;
}

int StatisticsManager::IncrementNumGets( short partitionID )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumGets++;
	if( m_setCounter )
		m_setCounter( PerfManager::NumGets, m_statistics[partitionID].NumGets, partitionID );
	RELEASE
	return 0;
}

int StatisticsManager::IncrementNumDeletes( short partitionID )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumDeletes++;
	if( m_setCounter )
		m_setCounter( PerfManager::NumDeletes, m_statistics[partitionID].NumDeletes, partitionID );
	RELEASE
	return 0;
}


int StatisticsManager::IncrementNumInserts( short partitionID )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumInserts++;
	if( m_setCounter )
		m_setCounter( PerfManager::NumInserts, m_statistics[partitionID].NumInserts, partitionID );
	RELEASE
	return 0;
}

int StatisticsManager::IncreaseNumEvictions( short partitionID, long numEvictions )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumEvictions += numEvictions;
	m_statistics[partitionID].WindowNumEvictions->addValue( numEvictions );
	if( m_setCounter )
		m_setCounter( PerfManager::NumEvictions, m_statistics[partitionID].NumEvictions, partitionID );
	if( m_setCounter )
		m_setCounter( PerfManager::WindowNumEvictions, m_statistics[partitionID].WindowNumEvictions->getTotalValue(), partitionID );
	RELEASE
	return 0;
}

int StatisticsManager::IncreaseNumVicimizeFailures( short partitionID, long numFailures )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumVictimizeFailures += numFailures;
	if( m_setCounter )
		m_setCounter( PerfManager::NumVictimizeFailures, m_statistics[partitionID].NumVictimizeFailures, partitionID );
	RELEASE
	return 0;
}

int StatisticsManager::IncreaseNumDeadlocks( short partitionID, long numDeadlocks )
{
	PARTITION_ID_VALIDITY_TEST
	ACQUIRE_WAIT
	m_statistics[partitionID].NumDeadlocks += numDeadlocks;
	if( m_setCounter )
		m_setCounter( PerfManager::NumDeadlocks, m_statistics[partitionID].NumDeadlocks, partitionID );
	RELEASE
	return 0;
}

// val is either 1 or -1 signifying increment or decrement respectively
int StatisticsManager::ModifyNumSynchronousInsert(short val)
{
	if(val==1)
		InterlockedIncrement(&m_numSynchronousInsert);
	else if(val ==-1)
		InterlockedDecrement(&m_numSynchronousInsert);

	return 0;
}

long StatisticsManager::GetNumSynchronousInsert () const
{
	return m_numSynchronousInsert;
}


int StatisticsManager::ResetPerfCounterStatistics( short partitionID )
{
	if( partitionID >= m_numOfPartition )
	{
		return -1;
	}
	else if( partitionID < 0 )
	{
		for( int i = 0; i < m_numOfPartition; i++ )
		{
			m_statistics[i].NumBytesAccessedTotal = 0;
			m_statistics[i].NumBytesCacheHit = 0;
			m_statistics[i].NumCacheHits = 0;
			m_statistics[i].NumDeadlocks = 0;
			m_statistics[i].NumEvictions = 0;
			m_statistics[i].NumGets = 0;
			m_statistics[i].NumDeletes = 0;
			m_statistics[i].NumInserts = 0;
			m_statistics[i].NumRequests = 0;
			m_statistics[i].NumRequestsKeyFound = 0;
			m_statistics[i].NumVictimizeFailures = 0;

			m_statistics[i].WindowBytesAccessedTotal->reset();
			m_statistics[i].WindowBytesCacheHit->reset();
			m_statistics[i].WindowNumCacheHits->reset();
			m_statistics[i].WindowNumEvictions->reset();
			m_statistics[i].WindowNumRequests->reset();
		}
	}
	else if( partitionID >= 0 )
	{
		int i = partitionID;
		m_statistics[i].NumBytesAccessedTotal = 0;
		m_statistics[i].NumBytesCacheHit = 0;
		m_statistics[i].NumCacheHits = 0;
		m_statistics[i].NumDeadlocks = 0;
		m_statistics[i].NumEvictions = 0;
		m_statistics[i].NumGets = 0;
		m_statistics[i].NumDeletes = 0;
		m_statistics[i].NumInserts = 0;
		m_statistics[i].NumRequests = 0;
		m_statistics[i].NumRequestsKeyFound = 0;
		m_statistics[i].NumVictimizeFailures = 0;

		m_statistics[i].WindowBytesAccessedTotal->reset();
		m_statistics[i].WindowBytesCacheHit->reset();
		m_statistics[i].WindowNumCacheHits->reset();
		m_statistics[i].WindowNumEvictions->reset();
		m_statistics[i].WindowNumRequests->reset();
	}

	return 0;
}

double StatisticsManager::GetCacheHitRate(short partitionID) const
{ 
	if( partitionID < 0 )
	{
		int totalRequests = 0;
		int totalCacheHits = 0;

		for( int i = 0; i < m_numOfPartition; i++ )
		{
			totalRequests += m_statistics[i].WindowNumRequests->getTotalValue();
			totalCacheHits += m_statistics[i].WindowNumCacheHits->getTotalValue();
		}
		
		return double(totalCacheHits) * 100.0 / totalRequests;
	}
	else
		return m_statistics[partitionID].WindowNumCacheHits->getTotalValue() / 
			double(m_statistics[partitionID].WindowNumRequests->getTotalValue()) * 100.0; 
}

double StatisticsManager::GetByteHitRate(short partitionID) const
{
	if( partitionID < 0 )
	{
		__int64 totalBytesAccessed = 0;
		__int64 totalBytesCacheHit = 0;

		for( int i = 0; i < m_numOfPartition; i++ )
		{
			totalBytesAccessed += m_statistics[i].WindowBytesAccessedTotal->getTotalValue();
			totalBytesCacheHit += m_statistics[i].WindowBytesCacheHit->getTotalValue();
		}
		
		return double(totalBytesCacheHit) * 100.0 / totalBytesAccessed;
	}
	else
		return m_statistics[partitionID].WindowBytesCacheHit->getTotalValue() / 
			double(m_statistics[partitionID].WindowBytesAccessedTotal->getTotalValue()) * 100.0; 
}

int StatisticsManager::GetNumQ(short partitionID) const
{ 
	if( partitionID < 0 )
	{
		int totalNumQ = 0;

		for( int i = 0; i < m_numOfPartition; i++ )
		{
			totalNumQ += m_statistics[i].numQ;
		}

		return totalNumQ;
	}
	else
		return m_statistics[partitionID].numQ; 
}

long long StatisticsManager::GetTotalBytes() const
{
	long long totalBytes = 0;

	for( int i = 0; i < m_numOfPartition; i++ )
	{
		totalBytes += m_statistics[i].cleanBytes;
		totalBytes += m_statistics[i].dirtyBytes;
	}

	return totalBytes;
}

long StatisticsManager::GetNumEvictions(short partitionID) const
{
	if( partitionID < 0 )
	{
		long totalNumEvictions= 0;

		for( int i = 0; i < m_numOfPartition; i++ )
		{
			totalNumEvictions+= m_statistics[i].NumEvictions;
		}
		return totalNumEvictions;
	}
	else
		return m_statistics[partitionID].NumEvictions; 
}

long StatisticsManager::GetNumVictimizeFailures (short partitionID) const
{
	if( partitionID < 0 )
	{
		long totalVictimizeFailures = 0;

		for( int i = 0; i < m_numOfPartition; i++ )
		{
			totalVictimizeFailures += m_statistics[i].NumVictimizeFailures;
		}
		return totalVictimizeFailures ;
	}
	else
		return m_statistics[partitionID].NumVictimizeFailures; 
}

long StatisticsManager::GetNumDeadlocks (short partitionID) const
{
	if( partitionID < 0 )
	{
		long totalNumDeadlocks = 0;

		for( int i = 0; i < m_numOfPartition; i++ )
		{
			totalNumDeadlocks += m_statistics[i].NumDeadlocks;
		}
		return totalNumDeadlocks;
	}
	else
		return m_statistics[partitionID].NumDeadlocks; 
}

double StatisticsManager::GetTotalCacheHitRate(short partitionID) const
{
	if (partitionID<0)
	{
		long totCacheHits=0;
		long totNumRequest=0;
		for (int i=0;i<m_numOfPartition;i++)
		{
			totNumRequest+= m_statistics[i].NumRequests;
			totCacheHits+= m_statistics[i].NumCacheHits;
		}	
		return (double) (totCacheHits * 100.0) / totNumRequest;
	}
	else
		return (double) m_statistics[partitionID].NumCacheHits*100/ m_statistics[partitionID].NumRequests;
}

double StatisticsManager::GetTotalByteHitRate(short partitionID) const
{
	if (partitionID<0)
	{
		long long totByteHits=0;
		long long totByteRequest=0;
		for (int i=0;i<m_numOfPartition;i++)
		{
			totByteRequest+= m_statistics[i].NumBytesAccessedTotal;
			totByteHits+= m_statistics[i].NumBytesCacheHit;
		}	
		return (double) (totByteHits * 100.0) / totByteRequest;
	}
	else
		return (double) m_statistics[partitionID].NumBytesCacheHit*100/ m_statistics[partitionID].NumBytesAccessedTotal;
}

