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

#include "stdafx.h"
#include "PerfManager.h"
#include "Common.h"

MyPerfMonManager PerfManager::m_perfMgr;
MyPerfObjects **PerfManager::m_pPerfObjInstArray = NULL;
int	PerfManager::m_numPerfObjInstances = 0;

int PerfManager::Initialize( int numPerfObjInstances )
{
	// 1 extra instance at the last element of the array for the total
	if( numPerfObjInstances > 1 )
		m_numPerfObjInstances = numPerfObjInstances;	
	else
		m_numPerfObjInstances = 1;

	m_pPerfObjInstArray = new MyPerfObjects*[m_numPerfObjInstances + 1];

	int ret = InitPerformanceCounters();

	return ret;
}

int	PerfManager::Shutdown()
{
	int ret = ReleasePerformanceCounters();

	if( m_pPerfObjInstArray != NULL )
		delete [] m_pPerfObjInstArray;

	return ret;
}

int PerfManager::InitPerformanceCounters()
{
	std::string instance_name_base = "Partition";
	std::string instance_name;
	if (SUCCEEDED(m_perfMgr.Initialize())) {
		CPerfLock lock(&m_perfMgr);
		if (SUCCEEDED(lock.GetStatus())) {
			for( int i = 0; i < m_numPerfObjInstances; i++ )
			{
				// Need to convert string to wide string
				instance_name = instance_name_base + TCUtility::intToString( i );
				if (SUCCEEDED(m_perfMgr.CreateInstanceByName(TCUtility::stringToWideString(instance_name).c_str(), &m_pPerfObjInstArray[i])))
				{
					ClearPerfCounters(m_pPerfObjInstArray[i]);
				}
				else 
				{
					//Error
					return -1;
				}
			}

			instance_name = "_Total";
			if (SUCCEEDED(m_perfMgr.CreateInstanceByName(TCUtility::stringToWideString(instance_name).c_str(), &m_pPerfObjInstArray[m_numPerfObjInstances])))
			{
				ClearPerfCounters(m_pPerfObjInstArray[m_numPerfObjInstances]);
			}
			else 
			{
				//Error
				return -1;
			}
		}
	}
	else
	{
		//Error Init
		printf("Error initializing counters");
		return -1;
	}

	return 0;
}

int PerfManager::ReleasePerformanceCounters()
{
	int ret = 0;
	MyPerfObjects *pPerfObj;

	for( int i = 0; i < m_numPerfObjInstances + 1; i++ )
	{
		if (m_pPerfObjInstArray[i] != NULL) {
			ClearPerfCounters(m_pPerfObjInstArray[i]);

			CPerfLock lock(&m_perfMgr);
			if (SUCCEEDED(lock.GetStatus())) {
				pPerfObj = m_pPerfObjInstArray[i];
				m_pPerfObjInstArray[i] = NULL;
				m_perfMgr.ReleaseInstance(pPerfObj);
			}
			else
			{
				//Error locking
				ret = -1;
			}
		}
	}

	m_perfMgr.UnInitialize();

	return ret;
}


void __stdcall PerfManager::IncreaseCounter ( const long& counterID, const long& value, const long& perfObjInstance )
{
	if( perfObjInstance >= m_numPerfObjInstances )
		return;

	switch( counterID )
	{
	case NumQ:
		m_pPerfObjInstArray[perfObjInstance]->NumQ += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumQ += value;
		break;
	case NumRequests:
		m_pPerfObjInstArray[perfObjInstance]->NumRequests += value;
		m_pPerfObjInstArray[perfObjInstance]->NumRequestsPerSecond += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumRequests += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumRequestsPerSecond += value;
		break;
	case NumCacheHits:
		m_pPerfObjInstArray[perfObjInstance]->NumCacheHits += value;
		break;
	case NumEvictions:
		break;
	case NumGets:
		break;
	case NumInserts:
		break;
	case NumDeletes:
		break;
	default:
		return;
	}
}

void __stdcall PerfManager::IncreaseLargeCounter ( const long& counterID, const long long& value, const long& perfObjInstance )
{
	if( perfObjInstance >= m_numPerfObjInstances )
		return;

	switch( counterID )
	{
	case NumBytesAccessedTotal:
		m_pPerfObjInstArray[perfObjInstance]->NumBytesAccessedTotal += value;
		m_pPerfObjInstArray[perfObjInstance]->NumBytesAccessedTotalPerSecond += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesAccessedTotal += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesAccessedTotalPerSecond += value;
		break;
	case NumBytesCacheHit:
		m_pPerfObjInstArray[perfObjInstance]->NumBytesCacheHit += value;
		m_pPerfObjInstArray[perfObjInstance]->NumBytesCacheHitPerSecond += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesCacheHit += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesCacheHitPerSecond += value;
		break;
	case CleanBytes:
		m_pPerfObjInstArray[perfObjInstance]->CleanBytes += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->CleanBytes += value;
		break;
	case DirtyBytes:
		m_pPerfObjInstArray[perfObjInstance]->DirtyBytes += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->DirtyBytes += value;
		break;
	case TotalBytes:
		m_pPerfObjInstArray[perfObjInstance]->TotalBytes += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->TotalBytes += value;
		break;
	case NumFreeSpaceBytes:
		m_pPerfObjInstArray[perfObjInstance]->NumFreeSpaceBytes += value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumFreeSpaceBytes += value;
		break;
	default:
		break;
	}

}

void __stdcall PerfManager::SetCounter( const long& counterID, const long& value, const long& perfObjInstance )
{
	//long prev_value;
	long delta_value;

	switch( counterID )
	{
	case NumQ:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumQ;
		m_pPerfObjInstArray[perfObjInstance]->NumQ = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumQ += delta_value;
		break;
	case NumRequestsKeyFound:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumRequestsKeyFound;
		m_pPerfObjInstArray[perfObjInstance]->NumRequestsKeyFound = value;
		m_pPerfObjInstArray[perfObjInstance]->NumRequestsKeyFoundPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumRequestsKeyFound += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumRequestsKeyFoundPerSecond += delta_value;
		break;
	case NumRequests:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumRequests;
		m_pPerfObjInstArray[perfObjInstance]->NumRequests = value;
		m_pPerfObjInstArray[perfObjInstance]->NumRequestsPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumRequests += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumRequestsPerSecond += delta_value;
		break;
	case NumCacheHits:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumCacheHits;
		m_pPerfObjInstArray[perfObjInstance]->NumCacheHits = value;
		m_pPerfObjInstArray[perfObjInstance]->NumCacheHitsPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumCacheHits += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumCacheHitsPerSecond += delta_value;
		break;	
	case NumEvictions:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumEvictions;
		m_pPerfObjInstArray[perfObjInstance]->NumEvictions = value;
		m_pPerfObjInstArray[perfObjInstance]->NumEvictionsPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumEvictions += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumEvictionsPerSecond += delta_value;
		break;
	case NumGets:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumGets;
		m_pPerfObjInstArray[perfObjInstance]->NumGets = value;
		m_pPerfObjInstArray[perfObjInstance]->NumGetsPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumGets += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumGetsPerSecond += delta_value;
		break;
	case NumInserts:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumInserts;
		m_pPerfObjInstArray[perfObjInstance]->NumInserts = value;
		m_pPerfObjInstArray[perfObjInstance]->NumInsertsPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumInserts += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumInsertsPerSecond += delta_value;
		break;
	case NumDeletes:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumDeletes;
		m_pPerfObjInstArray[perfObjInstance]->NumDeletes = value;
		m_pPerfObjInstArray[perfObjInstance]->NumDeletesPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumDeletes += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumDeletesPerSecond += delta_value;
		break;
	case WindowNumRequests:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->WindowNumRequests;
		m_pPerfObjInstArray[perfObjInstance]->WindowNumRequests = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->WindowNumRequests += delta_value;
		break;
	case WindowNumCacheHits:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->WindowNumCacheHits;
		m_pPerfObjInstArray[perfObjInstance]->WindowNumCacheHits = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->WindowNumCacheHits += delta_value;
		break;
	case WindowNumEvictions:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->WindowNumEvictions;
		m_pPerfObjInstArray[perfObjInstance]->WindowNumEvictions = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->WindowNumEvictions += delta_value;
		break;
	case AsynchronousMode:
		m_pPerfObjInstArray[perfObjInstance]->AsynchronousMode = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->AsynchronousMode = value;
		break;
	case NumVictimizeFailures:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumVictimizeFailures;
		m_pPerfObjInstArray[perfObjInstance]->NumVictimizeFailures = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumVictimizeFailures += delta_value;
		break;		
	case NumDeadlocks:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumDeadlocks;
		m_pPerfObjInstArray[perfObjInstance]->NumDeadlocks = value;
		m_pPerfObjInstArray[perfObjInstance]->NumDeadlocksPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumDeadlocks += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumDeadlocksPerSecond += delta_value;
		break;		
	default:
		return;
	}
}

void __stdcall PerfManager::SetLargeCounter( const long& counterID, const long long& value, const long& perfObjInstance )
{
	if( perfObjInstance >= m_numPerfObjInstances )
		return;

	long long delta_value;

	switch( counterID )
	{
	case NumBytesAccessedTotal:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumBytesAccessedTotal;
		m_pPerfObjInstArray[perfObjInstance]->NumBytesAccessedTotal = value;
		m_pPerfObjInstArray[perfObjInstance]->NumBytesAccessedTotalPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesAccessedTotal += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesAccessedTotalPerSecond += delta_value;
		break;
	case NumBytesCacheHit:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumBytesCacheHit;
		m_pPerfObjInstArray[perfObjInstance]->NumBytesCacheHit = value;
		m_pPerfObjInstArray[perfObjInstance]->NumBytesCacheHitPerSecond += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesCacheHit += delta_value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumBytesCacheHitPerSecond += delta_value;
		break;
	case WindowNumBytesAccessedTotal:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->WindowNumBytesAccessedTotal;
		m_pPerfObjInstArray[perfObjInstance]->WindowNumBytesAccessedTotal = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->WindowNumBytesAccessedTotal += delta_value;
		break;
	case WindowNumBytesCacheHit:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->WindowNumBytesCacheHit;
		m_pPerfObjInstArray[perfObjInstance]->WindowNumBytesCacheHit = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->WindowNumBytesCacheHit += delta_value;
		break;
	case NumFreeSpaceBytes:
		delta_value = value - m_pPerfObjInstArray[perfObjInstance]->NumFreeSpaceBytes;
		m_pPerfObjInstArray[perfObjInstance]->NumFreeSpaceBytes = value;
		m_pPerfObjInstArray[m_numPerfObjInstances]->NumFreeSpaceBytes += delta_value;
		break;
	default:
		break;
	}
}