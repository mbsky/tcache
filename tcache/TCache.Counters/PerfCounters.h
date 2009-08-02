#pragma once

#define _ATL_PERF_REGISTER

#include "stdafx.h"
#include <atlperf.h>


class MyPerfObjects : public CPerfObject
{
public:
	//This will appear in Perf Monitor in Categories drop down list
	DECLARE_PERF_CATEGORY(MyPerfObjects, 1, _T("Trojan Storage Manager"), _T("My Perf Category Help"), -1);

	//These are names of the counters and their respectfull description and help
	BEGIN_COUNTER_MAP(MyPerfObjects)
		DEFINE_COUNTER(NumQ, _T("01 NumQ"), _T("Number of records in QDB"), PERF_COUNTER_RAWCOUNT, 0)

		DEFINE_COUNTER(NumGets, _T("01 NumGets"), _T("Number of get operations made to the system"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumGetsPerSecond, _T("01 NumGetsPerSecond"), _T("Number of get operations serviced by the system in each second"), PERF_COUNTER_COUNTER, 0)
		DEFINE_COUNTER(NumInserts, _T("01 NumInserts"), _T("Number of insert operations made to the system"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumInsertsPerSecond, _T("01 NumInsertsPerSecond"), _T("Number of insert operations serviced by the system in each second"), PERF_COUNTER_COUNTER, 0)
		DEFINE_COUNTER(NumDeletes, _T("01 NumDeletes"), _T("Number of delete operations made to the system"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumDeletesPerSecond, _T("01 NumDeletesPerSecond"), _T("Number of delete operations serviced by the system in each second"), PERF_COUNTER_COUNTER, 0)

		DEFINE_COUNTER(NumRequests, _T("02 NumRequests"), _T("Number of requests made to the system"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumRequestsPerSecond, _T("02 NumRequestsPerSecond"), _T("Number of requests serviced by the system in each second"), PERF_COUNTER_COUNTER, 0)
		DEFINE_COUNTER(NumCacheHits, _T("02 NumCacheHits"), _T("Number of cache hits"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumCacheHits, _T("02 CacheHitRate"), _T("Cache hit rate"), PERF_RAW_FRACTION, 0)
		DEFINE_COUNTER(NumRequests, _T("02 CacheHitRateBase"), _T("CacheHitRate base"), PERF_RAW_BASE, 0)
		DEFINE_COUNTER(NumRequestsKeyFound, _T("02 NumRequestsKeyFound"), _T("Number of requests made to the system where the key was found"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumRequestsKeyFoundPerSecond, _T("02 NumRequestsKeyFoundPerSecond"), _T("Number of requests where the key was found serviced by the system in each second"), PERF_COUNTER_COUNTER, 0)
		DEFINE_COUNTER(NumCacheHits, _T("02 CacheHitRateKeyFound"), _T("Cache hit rate where key was found"), PERF_RAW_FRACTION, 0)
		DEFINE_COUNTER(NumRequestsKeyFound, _T("02 CacheHitRateKeyFoundBase"), _T("CacheHitRateKeyFound base"), PERF_RAW_BASE, 0)
		
		DEFINE_COUNTER(NumBytesAccessedTotal, _T("03 NumBytesAccessedTotal"), _T("Number of bytes accessed through disk and cache. If an object did not exist, it's byte count is not added"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(NumBytesCacheHit, _T("03 NumBytesCacheHit"), _T("Number of bytes accessed through cache"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(NumBytesCacheHit, _T("03 ByteHitRate"), _T("Byte hit rate"), PERF_LARGE_RAW_FRACTION, 0)
		DEFINE_COUNTER(NumBytesAccessedTotal, _T("03 ByteHitRateBase"), _T("ByteHitRate base"), PERF_LARGE_RAW_BASE, 0)
		
		DEFINE_COUNTER(NumEvictions, _T("04 NumEvictions"), _T("Number of evictions"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumEvictionsPerSecond, _T("04 NumEvictionsPerSecond"), _T("Number of evictions by the system in each second"), PERF_COUNTER_COUNTER, 0)
		DEFINE_COUNTER(NumVictimizeFailures, _T("04 NumVictimizeFailures"), _T("Number of times when victimize and insert still failed even after 10 tries"), PERF_COUNTER_RAWCOUNT, 0)

		DEFINE_COUNTER(CleanBytes, _T("05 CleanBytes"), _T("Number of bytes of clean objects in the cache"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(DirtyBytes, _T("05 DirtyBytes"), _T("Number of bytes of dirty objects in the cache"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(TotalBytes, _T("05 TotalBytes"), _T("Total number of bytes being used in the cache"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(DirtyBytes, _T("05 DirtyToTotalRatio"), _T("Ratio of bytes of dirty to total objects in the cache"), PERF_LARGE_RAW_FRACTION, 0)
		DEFINE_COUNTER(TotalBytes, _T("05 DirtyToTotalRatioBase"), _T("DirtyToCleanRatio base"), PERF_LARGE_RAW_BASE, 0)
		DEFINE_COUNTER(AsynchronousMode, _T("05 AsynchronousMode"), _T("Asynchronous mode 1, Synchronous mode 0"), PERF_COUNTER_RAWCOUNT, 0)
		

		DEFINE_COUNTER(WindowNumRequests, _T("06 SlidingWindowNumRequests"), _T("Number of requests made to the system within a certain sliding window"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(WindowNumCacheHits, _T("06 SlidingWindowNumCacheHits"), _T("Number of cache hits within a sliding window"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(WindowNumCacheHits, _T("06 SlidingWindowCacheHitRate"), _T("Cache hit rate within a sliding window"), PERF_RAW_FRACTION, 0)
		DEFINE_COUNTER(WindowNumRequests, _T("06 SlidingWindowCacheHitRateBase"), _T("WindowCacheHitRate base"), PERF_RAW_BASE, 0)
		DEFINE_COUNTER(WindowNumEvictions, _T("06 SlidingWindowNumEvictions"), _T("Number of evictions made by the system within a certain sliding window"), PERF_COUNTER_RAWCOUNT, 0)

		DEFINE_COUNTER(WindowNumBytesAccessedTotal, _T("07 SlidingWindowNumBytesAccessedTotal"), _T("Number of bytes accessed through disk and cache. If an object did not exist, it's byte count is not added"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(WindowNumBytesCacheHit, _T("07 SlidingWindowNumBytesCacheHit"), _T("Number of bytes accessed through cache within a sliding window"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		DEFINE_COUNTER(WindowNumBytesCacheHit, _T("07 SlidingWindowByteHitRate"), _T("Byte hit rate within a sliding window"), PERF_LARGE_RAW_FRACTION, 0)
		DEFINE_COUNTER(WindowNumBytesAccessedTotal, _T("07 SlidingWindowByteHitRateBase"), _T("WindowByteHitRate base"), PERF_LARGE_RAW_BASE, 0)

		DEFINE_COUNTER(NumFreeSpaceBytes, _T("08 NumFreeSpaceBytes"), _T("Number of bytes free in cache"), PERF_COUNTER_LARGE_RAWCOUNT, 0)

		DEFINE_COUNTER(NumDeadlocks, _T("09 NumDeadlocks"), _T("Number of deadlocks"), PERF_COUNTER_RAWCOUNT, 0)
		DEFINE_COUNTER(NumDeadlocksPerSecond, _T("09 NumDeadlocksPerSecond"), _T("Number of deadlocks in the system each second"), PERF_COUNTER_COUNTER, 0)

		//DEFINE_COUNTER(myCounter2, _T("myCounterName2"), _T("my counter2 help raw"), PERF_RAW_FRACTION, 0)
		//DEFINE_COUNTER(myCounter3, _T("myCounterName2Base"), _T("my counter2 help base"), PERF_RAW_BASE, 0)
		//DEFINE_COUNTER(myCounter2, _T("myCounterName2"), _T("my counter2 help per second"), PERF_COUNTER_COUNTER, 0)
		//DEFINE_COUNTER(myCounter3, _T("myCounterName3"), _T("my counter3 help larg raw"), PERF_COUNTER_LARGE_RAWCOUNT, 0)
		//DEFINE_COUNTER(myCounter4, _T("myCounterName4"), _T("my counter4 help larg raw"), PERF_AVERAGE_BULK, 0)
		//DEFINE_COUNTER(myCounter5, _T("myCounterName5 Base"), _T("my counter5 help larg raw"), PERF_AVERAGE_BASE, 0)
	END_COUNTER_MAP()

	__int64 startOfCounters;

	long	NumQ;
	long	NumRequests;
	long	NumCacheHits;
	long	NumRequestsKeyFound;
	__int64	NumBytesAccessedTotal;
	__int64	NumBytesCacheHit;	
	long	NumEvictions;
	long	NumGets;
	long	NumInserts;
	long	NumDeletes;
	__int64	CleanBytes;
	__int64 DirtyBytes;
	__int64 TotalBytes;
	long	AsynchronousMode;
	long	NumDeadlocks;


	long	NumRequestsPerSecond;
	long	NumCacheHitsPerSecond;
	long	NumRequestsKeyFoundPerSecond;
	__int64	NumBytesAccessedTotalPerSecond;
	__int64	NumBytesCacheHitPerSecond;
	long	NumEvictionsPerSecond;
	long	NumGetsPerSecond;
	long	NumInsertsPerSecond;
	long	NumDeletesPerSecond;
	long	NumDeadlocksPerSecond;

	long	WindowNumRequests;
	long	WindowNumCacheHits;
	long	WindowNumEvictions;
	__int64	WindowNumBytesAccessedTotal;
	__int64	WindowNumBytesCacheHit;

	__int64 NumFreeSpaceBytes;
	long	NumVictimizeFailures;



	//long myCounter2;
	//long myCounter3;
	//__int64 myCounter4;
	//long myCounter5;

	long endOfCounters;
};

class MyPerfMonManager : public CPerfMon
{
public:
	BEGIN_PERF_MAP(_T("MyTrojanPerfMonManager"))	//This name will appear in Registry: under HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\ 
		CHAIN_PERF_CATEGORY(MyPerfObjects)
		//CHAIN_PERF_OBJECT(MyPerfObjects)
	END_PERF_MAP()
};

PERFREG_ENTRY(MyPerfMonManager);

template <class T> void ClearPerfCounters(T *pPerfObj)
{
	memset(&pPerfObj->startOfCounters, 0,
		(char *)&pPerfObj->endOfCounters - (char *)&pPerfObj->startOfCounters);
}