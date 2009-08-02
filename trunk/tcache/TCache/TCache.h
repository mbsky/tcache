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

#ifndef V_CACHE_MANAGER__
#define V_CACHE_MANAGER__

#ifndef V_CACHE_MANAGER_NEW_GET__
#define V_CACHE_MANAGER_NEW_GET__
#endif

#include <iostream>
#include "Common.h"
#include "ConfigParameters.h"
#include "Vdt.h"
#include "ContextManager.h"
#include "CacheDatabase.h"
#include "QDatabase.h"
#include "CachingAlgorithm.h"
#include "StatisticsManager.h"
#include "DiskBDB.h"
#include "TCLock.h"
#include "LoggerStream.h"
#include "TCAging.h"
#include "TCQValue.h"
#include <list>


struct MDStructHist
{
	char oldDirtyBit;
	TCQValue oldQValue;
	TCQValue newQValue;
	double avgQValue;
	double LValue;
	int lastEvictedAgeValue;
	int recordSize;
	int newRecordSize;
	LARGE_INTEGER currTimestamp;
	LARGE_INTEGER lastUpdateTimestamp;
	ContextStructure *c_struct;
};

class _export TCache
{
private:
	ConfigParametersV m_configParameters;
	DbEnv			*m_memoryDbEnv;

	DbEnv			*m_diskDbEnv;

	TCLock			*m_lock;
	ContextManager	*m_contextManager;
	CachingAlgorithm	*m_cachingAlgorithm;

	std::list<const Vdt*> m_truncateDZList;

	LoggerStream	*m_logStream;
	TCAging		*m_agingMechanism;

	LARGE_INTEGER	m_CONST_updateDelta;
	int				m_numPartitions;
	volatile long	m_shutdownFlag;
	

	int				m_MaxVictimSelectionByRecordsMultiple;
	int				m_MaxVictimSelectionSizeMultiple;
	//int				m_NumEvictionsPerOpenCursor;
	int				m_MaxVictimSelectionAttempts;

	
	HANDLE			m_semaphore;
	HANDLE			m_backgroundThreadHandle;  
	HANDLE			m_semaphoreDatazoneDelete;
	HANDLE			m_backgroundThreadDatazoneDeleteHandle;
	char *			m_backgroundThreadBuffer;
	int				m_backgroundThreadBufferSize;
	short			m_numDiskPartitions;

	double			m_asynchronousToSynchronousRatio;
	int				m_maxSynchronousInserts;

	long			m_num_signal;
	long			m_num_clean_to_dirty;

	long long		m_cacheSize; // Used in Background thread if L=0 

	//Victimize Stats counters
	long			m_num_victimizebyrecords;
	long			m_num_victimizebysize;

	bool			m_MemoryEnabled;
	bool			m_DiskEnabled;

	CacheDatabase	*m_cacheDB;
	QDatabase		*m_qDB;	
	DatazoneManager	*m_dzManager;
	DiskBDB			*m_diskDB;

	StatisticsManager	*m_statsManager;

private:
	int		setupMemoryEnv( const ConfigParametersV& cp );
	int		setupDiskEnv( const ConfigParametersV& cp );

	int		Victimize( const int& targetToEvict, const int& evictTypeFlag, const int& partition_ID, stats& stats_update );
	int		EvictAndInsertDataRec( CacheKey& cKey, CacheValue& cValue, const int& partition_ID, stats& stats_update );
	int		EvictAndInsertMDLookup( CacheKey& cKey, CacheValue& cValue, const int& partition_ID, stats& stats_update );
	int		EvictAndInsertQDBRec( QDBKey& qKey, QDBValue& qValue, const int& partition_ID, stats& stats_update );

	int		UpdateStats( const stats& stats_update, const int& partition_ID );
	int		VerifyConfigParameters( const ConfigParametersV& cp );

	int		GetFromCache( const Vdt *dzName, const Vdt &Key, Vdt *Value, const int& partition_ID );
	int		GetFromDisk( const Vdt *dzName, const Vdt &Key, Vdt *Value, const int& partition_ID );

	int		GetLowestDirtyQObjectFromAllQDBS(QDBKey &min_key,QDBValue &min_value,int  &min_partitionID) ;
	int		BackgroundThread_RMW();
	
	int		DeleteCacheKeysByDZ( const Vdt* dzName, const int KEY_RANGE_FLAG );

	// Internal Whitebox testing functions
	int		testEvictions();
	
	// NoCacheDBK1 = true if no data rec 
	// NoCacheDBK2 = true if no metadata lookup rec 
	// CacheK2CleanDirtyQDB =true Insert the Cache DBK2 as clean and QDB as dirty 
	// additionalQDBRec=true Insert an additional QDB record
	int		InsertNoCache(const Vdt& dzname, const Vdt& Key, const Vdt& Value,bool noCacheDBK1,bool noCacheDBK2,bool cacheK2CleanDirtyQDB, bool additionalQDBRec);
	// The boolean specify what sould exist
	bool	ExistsInCache(const Vdt& dzname, const Vdt& Key,  Vdt& Value,bool eCacheDBK1,bool eCacheDBK2,bool eQDB, bool eDisk);

	int TruncateDZThread_W( const Vdt *dzName );
	
	
	static	DWORD WINAPI  BackgroundThread( LPVOID lpParam ) ;

	static	DWORD WINAPI TruncateDZThread( LPVOID lpParam ) ;
	
	int		AsynchronousInsert (const Vdt& dzname, const Vdt& Key, const Vdt& valueSize);
	int		SynchronousInsert (const Vdt& dzname, const Vdt& Key, const Vdt& Value, int cachePartitionID, bool DoAdmissionControl,bool diskInsert=true, const double& cost = 1.0);
	
	int GetHistoryMD( const Vdt &Key, const Vdt *Value, const Vdt *dzName, MDStructHist *md_struct, const int& partition_ID, const int& MAINTAIN_MD );
	int UpdateMD( const Vdt &Key, const Vdt *Value, const Vdt *dzName, MDStructHist *md_struct, const int& partition_ID, const int& ADMITED_KEY_FLAG, stats& stats_update );

	int		DeleteAllKeysFromDZone( const Vdt* dzName ); 
public:
	TCache();
	~TCache();

	int		Initialize( const ConfigParametersV& cp );
	int		Shutdown();

	int		Create( const Vdt* dzName );

	int		Insert( const Vdt* dzName, const Vdt& Key, const Vdt& Value, const double& cost = 1.0 );
	int		Get( const Vdt* dzName, const Vdt& Key, Vdt* Value );
	int		Delete( const Vdt* dzName, const Vdt& Key );

	void	PrintStats( std::ostream& out );
	void	DumpCache( std::ostream& out );

	int		ResetPerfCounterStats() { return m_statsManager->ResetPerfCounterStatistics(-1); }
	ConfigParametersV GetConfigurationParams() { return m_configParameters; }
	const StatisticsManager* GetStatisticsManager() { return m_statsManager; }
	int		runInternalTests();

	//static	LARGE_INTEGER m_start,m_end,m_num_ticks;	
};


#endif // V_CACHE_MANAGER__
