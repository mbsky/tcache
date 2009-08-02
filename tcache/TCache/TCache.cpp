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
#include <cmath>
#include "TCache.h"


//LARGE_INTEGER TCache::m_start,TCache::m_end,TCache::m_num_ticks;

//extern uLong compressLen,unCompressLen; 
//extern char *compressBuffer,*unCompressBuffer;

TCache::TCache()
{

}


TCache::~TCache()
{

}


int TCache::Initialize( const ConfigParametersV &cp )
{
	m_shutdownFlag = 0;
	m_logStream = new LoggerStream();

	m_MemoryEnabled = cp.MemBDB.Enabled;
	m_DiskEnabled = cp.DiskBDB.Enabled;

	if( cp.PrintConfiguration )
	{
		cout<< "Configuration parameters:-\n";
		cout<< "PrintConfiguration: ";
		if( cp.PrintConfiguration )
			cout<< "true";
		else
			cout<< "false";
		cout<< "\n";

		cout<< "GrayPutzolu: " << cp.AgingInterval << " milliseconds\n";


		cout<< "AsyncWrites:\n";
		cout<< "\t" << "Enabled: ";
		if( cp.AsyncWrites.Enabled )
			cout<< "true";
		else
			cout<< "false";
		cout<< "\n";
		cout<< "\t" << "MaxSynchronousInserts: " << cp.AsyncWrites.MaxSynchronousInserts << "\n";
		cout<< "\t" << "AsynchronousToSynchronousRatio: " << cp.AsyncWrites.AsynchronousToSynchronousRatio * 100.0 << " %\n";
		
		cout<< "MemBDB: ";

		if( !cp.MemBDB.Enabled )
		{
			cout<< "Disabled\n";
		}
		else
		{
			cout<< "Enabled\n";
		
			cout<< "\t" << "VictimizationPolicies:\n";
			cout<< "\t" << "\t" << "MaxVictimSelectionSizeMultiple: " << cp.MemBDB.VictimizationPolicies.MaxVictimSelectionSizeMultiple << "\n";
			cout<< "\t" << "\t" << "MaxVictimSelectionByRecordsMultiple: " << cp.MemBDB.VictimizationPolicies.MaxVictimSelectionByRecordsMultiple << "\n";
			cout<< "\t" << "\t" << "MaxVictimSelectionAttempts: " << cp.MemBDB.VictimizationPolicies.MaxVictimSelectionAttempts << "\n";
			
			cout<< "\t" << "ReplacementTechniqueID: " << cp.MemBDB.ReplacementTechniqueID << "\n";
			
			cout<< "\t" << "CorrelatedRefDelta: " << cp.MemBDB.CorrelatedRefDelta << " milliseconds\n";
			
			cout<< "\t" << "CacheSize:\n";
			cout<< "\t" << "\t" << "Gigabytes: " << cp.MemBDB.CacheSize.GigaBytes << "\n";
			cout<< "\t" << "\t" << "Bytes: " << cp.MemBDB.CacheSize.Bytes << "\n";
			cout<< "\t" << "\t" << "NumberOfCaches: " << cp.MemBDB.CacheSize.NumberCaches << "\n";
			
			cout<< "\t" << "MaxLockers: ";
			if( cp.MemBDB.MaxLockers > 0 )
				cout<< cp.MemBDB.MaxLockers << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxLocks: ";
			if( cp.MemBDB.MaxLocks > 0 )
				cout<< cp.MemBDB.MaxLocks << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxLockObjects: ";
			if( cp.MemBDB.MaxLockObjects > 0 )
				cout<< cp.MemBDB.MaxLockObjects << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MutexIncrement: ";
			if( cp.MemBDB.MutexIncrement > 0 )
				cout<< cp.MemBDB.MutexIncrement << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxMutexes: ";
			if( cp.MemBDB.MaxMutexes > 0 )
				cout<< cp.MemBDB.MaxMutexes << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "LogSize: " << cp.MemBDB.LogSize << " bytes\n";

			cout<< "\t" << "DatabaseConfigs:\n";
			cout<< "\t" << "\t" << "Partitions: " << cp.MemBDB.DatabaseConfigs.Partitions << "\n";
			
			cout<< "\t" << "\t" << "CacheDBPageSize: ";
			if( cp.MemBDB.DatabaseConfigs.CacheDBPageSize > 0 )
				cout<< cp.MemBDB.DatabaseConfigs.CacheDBPageSize << " bytes\n";
			else
				cout<< "default\n";
				
			cout<< "\t" << "\t" << "QDBPageSize: ";
			if( cp.MemBDB.DatabaseConfigs.QDBPageSize > 0 )
				cout<< cp.MemBDB.DatabaseConfigs.QDBPageSize << " bytes\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxDeadlockRetries: " << cp.MemBDB.MaxDeadlockRetries << " \n";
		}


		cout<< "DiskBDB: ";
		if( !cp.DiskBDB.Enabled )
		{
			cout<< "Disabled\n";
		}
		else
		{
			cout<< "Enabled\n";

			cout<< "\t" << "CacheSize:\n";
			cout<< "\t" << "\t" << "Gigabytes: " << cp.DiskBDB.CacheSize.GigaBytes << "\n";
			cout<< "\t" << "\t" << "Bytes: " << cp.DiskBDB.CacheSize.Bytes << "\n";
			cout<< "\t" << "\t" << "NumberOfCaches: " << cp.DiskBDB.CacheSize.NumberCaches << "\n";
			
			cout<< "\t" << "MaxLockers: ";
			if( cp.DiskBDB.MaxLockers > 0 )
				cout<< cp.DiskBDB.MaxLockers << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxLocks: ";
			if( cp.DiskBDB.MaxLocks > 0 )
				cout<< cp.DiskBDB.MaxLocks << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxLockObjects: ";
			if( cp.DiskBDB.MaxLockObjects > 0 )
				cout<< cp.DiskBDB.MaxLockObjects << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MutexIncrement: ";
			if( cp.DiskBDB.MutexIncrement > 0 )
				cout<< cp.DiskBDB.MutexIncrement << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxMutexes: ";
			if( cp.DiskBDB.MaxMutexes > 0 )
				cout<< cp.DiskBDB.MaxMutexes << "\n";
			else
				cout<< "default\n";

			cout<< "\t" << "InMemoryLogging:\n";
			cout<< "\t" << "\t" << "Enabled: ";
			if( cp.DiskBDB.InMemoryLogging.Enabled )
				cout<< "true";
			else
				cout<< "false";
			cout<< "\n";
			cout<< "\t" << "\t" << "LogSize: " << cp.DiskBDB.InMemoryLogging.LogSize << " bytes\n";

			cout<< "\t" << "DatabaseConfigs:\n";
			cout<< "\t" << "\t" << "Partitions: " << cp.DiskBDB.DatabaseConfigs.Partitions << "\n";
			cout<< "\t" << "\t" << "PageSize: ";
			if( cp.DiskBDB.DatabaseConfigs.PageSize > 0 )
				cout<< cp.DiskBDB.DatabaseConfigs.PageSize << " bytes\n";
			else
				cout<< "default\n";

			cout<< "\t" << "MaxDeadlockRetries: " << cp.DiskBDB.MaxDeadlockRetries << " \n";
		}
	}


	if( !m_MemoryEnabled )
	{
		m_numPartitions = 1;
		m_memoryDbEnv = NULL;
		m_cacheDB = NULL;
		m_qDB = NULL;
		m_cachingAlgorithm = NULL;
		m_agingMechanism = NULL;
	}
	else
	{
		m_numPartitions = cp.MemBDB.DatabaseConfigs.Partitions;

		// TODO: its using the aging interval as init value for now to test out some stuff
		//m_agingMechanism = new TCAging( 10, cp.AgingInterval );
		m_agingMechanism = new TCAging( cp.AgingInterval, 0 );

		if( setupMemoryEnv( cp ) != 0 )
			m_logStream->print( "Error. Could not set up in-memory environment\n" );

		m_cacheDB = new CacheDatabase();
		m_cacheDB->Initialize( m_memoryDbEnv, cp );

		m_qDB = new QDatabase();
		m_qDB->Initialize( m_memoryDbEnv, cp );

		switch( cp.MemBDB.ReplacementTechniqueID )
		{
		case 1:
			m_cachingAlgorithm = new GreedyDualSizeAlgorithm( &cp );
			break;
		case 2:
			m_cachingAlgorithm = new LRUAlgorithm( &cp );
			break;
		case 3:
			m_cachingAlgorithm = new LRU2Algorithm( &cp );
			break;
		case 4:
			m_cachingAlgorithm = new IntervalBasedGreedyDualAlgorithm( &cp );
			break;
		case 5:
			m_cachingAlgorithm = new LRU_SKAlgorithm( &cp );
			break;

		case 12:
			m_cachingAlgorithm = new LRUTimeAlgorithm( &cp );
			break;
		case 13:
			m_cachingAlgorithm = new LRU2TimeAlgorithm( &cp );
			break;
		case 14:
			m_cachingAlgorithm = new IntervalBasedGreedyDualTimeAlgorithm( &cp );
			break;
		case 15:
			m_cachingAlgorithm = new LRU_SKTimeAlgorithm( &cp );
			break;

		default:
			// By default, the Interval based Greedy Dual Algorithm is used
			m_cachingAlgorithm = new IntervalBasedGreedyDualAlgorithm( &cp );
			break;
		}

		m_MaxVictimSelectionByRecordsMultiple = cp.MemBDB.VictimizationPolicies.MaxVictimSelectionByRecordsMultiple;
		m_MaxVictimSelectionSizeMultiple = cp.MemBDB.VictimizationPolicies.MaxVictimSelectionSizeMultiple;
		//m_NumEvictionsPerOpenCursor = cp.MemBDB.VictimizationPolicies.NumEvictionsPerOpenCursor;
		m_MaxVictimSelectionAttempts = cp.MemBDB.VictimizationPolicies.MaxVictimSelectionAttempts;

		// Set the update delta. Updates that occur within the delta are ignored.
		LARGE_INTEGER freq;
		QueryPerformanceFrequency( &freq );
		m_CONST_updateDelta.QuadPart = cp.MemBDB.CorrelatedRefDelta * (freq.QuadPart / 1000);
	}

	if( !m_DiskEnabled )
	{
		m_diskDB = NULL;
		m_numDiskPartitions = 1;
	}
	else
	{
		if( setupDiskEnv( cp ) != 0 )
			m_logStream->print( "Error. Could not set up in-memory environment\n" );

		m_numDiskPartitions=cp.DiskBDB.DatabaseConfigs.Partitions;
		m_dzManager = new DatazoneManager();
		m_dzManager->Initialize( m_diskDbEnv, cp );

		m_diskDB = new DiskBDB();
		m_diskDB->Initialize( m_diskDbEnv, m_dzManager, cp );
	}

	m_contextManager = new ContextManager( 4, 4, 8 );
	m_lock = new TCLock( 1000, 4 );	

	m_statsManager = new StatisticsManager( m_numPartitions, m_lock );

	
	m_num_signal=0;
	m_num_clean_to_dirty=0;
	m_asynchronousToSynchronousRatio=0; 

	// Background thread/Async writing is only enabled if both memory and disk are enabled
	if( m_MemoryEnabled && m_DiskEnabled && cp.AsyncWrites.Enabled==true)
	{
		// Create the semaphores,
		m_semaphore= CreateSemaphore(
			NULL,		// no security attributes
			0,			// initial count
			TC_MAX_QUEUED_INSERTS,    // maximum count
			NULL);		// non named semaphore

		if ( !m_semaphore) { 
			m_logStream->print("Error, failed to setup the synchronization mechanism(semaphore).\n");
			return -1; 
		}
		
		m_backgroundThreadHandle = CreateThread( NULL, 0, (LPTHREAD_START_ROUTINE)BackgroundThread, (LPVOID)this, 0, NULL);
		
		if(m_backgroundThreadHandle==NULL)
		{
			m_logStream->print("Error, failed to create Background Thread\n");
			return -1;
		}

		m_backgroundThreadBufferSize= 1024 * 1024; // Default 1MB
		m_asynchronousToSynchronousRatio=cp.AsyncWrites.AsynchronousToSynchronousRatio;
		m_maxSynchronousInserts=cp.AsyncWrites.MaxSynchronousInserts;


		// Create the semaphores,
		m_semaphoreDatazoneDelete= CreateSemaphore(
			NULL,   // no security attributes
			0,    // initial count
			10,    // maximum count
			NULL);  // non named semaphore

		if ( !m_semaphore) { 
			m_logStream->print("Error, failed to setup the synchronization mechanism(semaphore).\n");
			return -1; 
		}
		
		m_backgroundThreadDatazoneDeleteHandle =  CreateThread( NULL, 0, (LPTHREAD_START_ROUTINE)TruncateDZThread, (LPVOID)this, 0, NULL);
		if(m_backgroundThreadDatazoneDeleteHandle ==NULL)
		{
			m_logStream->print("Error, failed to create Background Thread Datazone Delete\n");
			return -1;
		}
	}	

	m_configParameters = cp;

	m_num_victimizebyrecords=0;
	m_num_victimizebysize=0;
	return 0;
}


int TCache::Shutdown()
{
	InterlockedIncrement( &m_shutdownFlag );
	

	// Background thread/Async writing is only enabled if both memory and disk are enabled
	if( m_MemoryEnabled && m_DiskEnabled && m_configParameters.AsyncWrites.Enabled==true)
	{
		//Release the semaphore to avoid an infinite wait. 
		if (!ReleaseSemaphore(m_semaphore,  // handle to semaphore
								1,                      // increase count by one
								NULL))            // not interested in previous count
		{
			printf("ReleaseSemaphore() failed, error: %d.\n", GetLastError());
			return -1;
		}

		WaitForSingleObject(m_backgroundThreadHandle,INFINITE);
		CloseHandle(m_semaphore);
		CloseHandle(m_backgroundThreadHandle);

		//Release the semaphore to avoid an infinite wait. 
		if (!ReleaseSemaphore(m_semaphoreDatazoneDelete,  // handle to semaphore
								1,                      // increase count by one
								NULL))            // not interested in previous count
		{
			printf("Release_m_semaphoreDatazoneDelete() failed, error: %d.\n", GetLastError());
			return -1;
		}

		WaitForSingleObject(m_backgroundThreadDatazoneDeleteHandle,INFINITE);
		CloseHandle(m_semaphoreDatazoneDelete);
		CloseHandle(m_backgroundThreadDatazoneDeleteHandle);

	}

	ContextStructure* cs=NULL;
	m_contextManager->GetFreeContext(cs);
	

	//m_logStream->print("Number of signals "+ TCUtility::intToString(m_num_signal)+"\n");
	//m_logStream->print("Number of clean to dirty "+ TCUtility::intToString(m_num_clean_to_dirty)+"\n");
	
	//printf("Number of signals %d  Clean to dirty %d\n",m_num_signal,m_num_clean_to_dirty);
	// Sanity Test for the background thread 
	//check if there are no dirty records
	QDBKey key(cs);
	QDBValue value(cs);
	int partitionID;
	bool santiyCheckPassed=true;
	// Set the minimum Q to the first object found in all the QDBs

	if( m_MemoryEnabled && m_DiskEnabled )
	{
		for(int i=0;i<m_numPartitions;i++)
		{
			if(m_statsManager->GetDirtyBytes(i)>0)
			{
				santiyCheckPassed=false;
				int ret=m_qDB->GetLowestDirtyRecord( key, value, i );
				if(ret!=0)
					continue;
				partitionID=i;
				printf("Key is %d and partition id %d\n",*(int *)(key.getKey().get_data()),partitionID);
			}
		}
		m_contextManager->ReleaseContext(cs);
		if(!santiyCheckPassed)
			m_logStream->print("Background Thread Error:: there are dirty objects in cache\n");
	}
	
	long numQ= m_statsManager->GetNumQ();
	long numEvictions= m_statsManager->GetNumEvictions();
	long numEvictionsFailure = m_statsManager->GetNumVictimizeFailures();
	long numDeadlocks = m_statsManager->GetNumDeadlocks();
	double cacheHit = m_statsManager->GetTotalCacheHitRate();
	double byteHit = m_statsManager->GetTotalByteHitRate();


	delete m_statsManager;

	delete m_lock;

	delete m_contextManager;

	if( m_DiskEnabled )
	{		
		m_diskDB->Shutdown();
		delete m_diskDB;

		m_dzManager->Shutdown();
		delete m_dzManager;

		m_diskDbEnv->close( 0 );
		delete m_diskDbEnv;
	}

	if( m_MemoryEnabled )
	{
		delete m_agingMechanism;

		delete m_cachingAlgorithm;

		m_qDB->Shutdown();
		delete m_qDB;

		m_cacheDB->Shutdown();
		delete m_cacheDB;

		m_memoryDbEnv->close( 0 );
		delete m_memoryDbEnv;
	}

	//if( m_fileStream )
	//	delete m_fileStream;

	delete m_logStream;

	/*
	//QueryPerformanceCounter(&TCache::m_end);
	//cout<< "Total time taken: " << (double)(TCache::m_end.QuadPart - TCache::m_start.QuadPart)/TCache::m_num_ticks.QuadPart * 1000 << " miliseconds\n";	

	
	ofstream fout;
	fout.open( "Results.txt", ofstream::out | ofstream::app );
	fout<<numQ<<" "<<cacheHit<<" "<<byteHit<<"\n";

	//fout<< (double)(TCache::m_end.QuadPart - TCache::m_start.QuadPart)/TCache::m_num_ticks.QuadPart <<",";
	//fout<<numQ<<","<<numEvictions<<","<<numEvictionsFailure<<","<<numDeadlocks<<"\n";
	fout.close();
	*/

	return 0;
}

int TCache::setupMemoryEnv( const ConfigParametersV& cp )
{
	u_int32_t open_flags = 0;
	int ret;

	m_memoryDbEnv = new DbEnv( 0 );

	open_flags =
		DB_CREATE     |  /* Create the environment if it does not exist */
		DB_INIT_LOCK  |  /* Initialize the locking subsystem */
		DB_INIT_LOG   |  /* Initialize the logging subsystem */
		DB_INIT_MPOOL |  /* Initialize the memory pool (in-memory cache) */
		DB_INIT_TXN   |
		DB_THREAD	  |
		DB_PRIVATE;      /* Region files are not backed by the filesystem. 
						  * Instead, they are backed by heap memory.  */


	/* Specify in-memory logging */
	ret = m_memoryDbEnv->log_set_config( DB_LOG_IN_MEMORY, 1 );
	if (ret != 0) 
	{
		fprintf(stderr, "Error setting log subsystem to in-memory: %s\n",
			db_strerror(ret));
	}

	ret = m_memoryDbEnv->set_lk_detect( DB_LOCK_MINWRITE );
	if (ret != 0) 
	{
		fprintf(stderr, "Error setting deadlock flag in-memory: %s\n",
			db_strerror(ret));
	}

	/* 
	 * Specify the size of the in-memory log buffer. 
	 */
	ret = m_memoryDbEnv->set_lg_bsize( cp.MemBDB.LogSize );
	if (ret != 0) 
	{
		fprintf(stderr, "Error increasing the log buffer size: %s\n",
			db_strerror(ret));
	}

	ret = 0;
	if( cp.MemBDB.MaxLockObjects > 0 )
		ret = m_memoryDbEnv->set_lk_max_objects( cp.MemBDB.MaxLockObjects );

	if( cp.MemBDB.MaxLocks > 0 )
		ret = ret + m_memoryDbEnv->set_lk_max_locks( cp.MemBDB.MaxLocks );

	if( cp.MemBDB.MaxLockers > 0 )
		ret = ret + m_memoryDbEnv->set_lk_max_lockers( cp.MemBDB.MaxLockers );

	if (ret != 0) 
	{
		fprintf(stderr, "Error increasing the max number of locks and lock objects: %s\n",
			db_strerror(ret));
	}

	if( cp.MemBDB.MutexIncrement > 0 )
	{
		ret = m_memoryDbEnv->mutex_set_increment( cp.MemBDB.MutexIncrement );
		if (ret != 0) {
			fprintf(stderr, "Error setting the mutex increment: %s\n",
				db_strerror(ret));
		}
	}

	if( cp.MemBDB.MaxMutexes > 0 )
	{
		ret = m_memoryDbEnv->mutex_set_max( cp.MemBDB.MaxMutexes );
		if (ret != 0) 
		{
			fprintf(stderr, "Error setting the max mutexes: %s\n",
				db_strerror(ret));
		}
	}

	/* 
	 * Specify the size of the in-memory cache. 
	 */
	ret = m_memoryDbEnv->set_cachesize(  cp.MemBDB.CacheSize.GigaBytes, 
		cp.MemBDB.CacheSize.Bytes, cp.MemBDB.CacheSize.NumberCaches );
	if (ret != 0) 
	{
		fprintf(stderr, "Error increasing the cache size: %s\n",
			db_strerror(ret));
	}

	m_cacheSize= cp.MemBDB.CacheSize.GigaBytes *1024*1024 + cp.MemBDB.CacheSize.Bytes ;
	

	// Tweaks BerkeleyDB to not stall for 3 seconds when it runs out of memory
	m_memoryDbEnv->set_flags( DB_DISABLE_LRU, 1 );


	/* 
	 * Now actually open the environment. Notice that the environment home
	 * directory is NULL. This is required for an in-memory only
	 * application. 
	 */
	ret = m_memoryDbEnv->open( NULL, open_flags, 0 );
	if (ret != 0) 
	{
		fprintf(stderr, "Error opening environment: %s\n",
			db_strerror(ret));
	}

	return ret;
}

int TCache::setupDiskEnv( const ConfigParametersV& cp )
{
	int ret = -1;

	u_int32_t open_flags = 0;
	open_flags |= DB_CREATE;
	open_flags |= DB_THREAD;
	open_flags |= DB_INIT_MPOOL;
	open_flags |= DB_INIT_TXN |DB_LOG_AUTO_REMOVE;
	open_flags |= DB_INIT_LOG|DB_INIT_LOCK;

	try {
		m_diskDbEnv = new DbEnv( 0 );

		// Turn on flags
		// Specify that operations in this environment are transactional
		ret = m_diskDbEnv->set_flags( DB_AUTO_COMMIT, 1 );
		if( ret != 0 )
		{
			fprintf(stderr, "Error setting DB_AUTO_COMMIT for disk: %s\n",
			db_strerror(ret));
		}


		// Specify that the system (OS) should not double-cache data loaded from disk
		ret = m_diskDbEnv->set_flags( DB_DIRECT_DB, 1 );
		if( ret != 0 )
		{
			fprintf(stderr, "Error setting DB_DIRECT_DB for disk: %s\n",
			db_strerror(ret));
		}

		if(cp.DiskBDB.InMemoryLogging.Enabled)
		{
			ret = m_diskDbEnv->log_set_config(DB_LOG_IN_MEMORY, 1);
			ret |= m_diskDbEnv->set_lg_bsize(cp.DiskBDB.InMemoryLogging.LogSize);
			if( ret != 0 )
			{
				fprintf(stderr, "Error configuring in-memory logging for disk: %s\n",
				db_strerror(ret));
			}
		}

		// Note: The directory structure has to be created before-hand,
		//		 otherwise BDB will have a dir not found error.
		ret = m_diskDbEnv->set_data_dir( cp.DiskBDB.DataDirectory.c_str() );
		if( ret != 0 )
		{
			fprintf(stderr, "Error setting data directory for disk: %s\n",
			db_strerror(ret));
		}
		
		ret = m_diskDbEnv->set_cachesize( cp.DiskBDB.CacheSize.GigaBytes,cp.DiskBDB.CacheSize.Bytes,cp.DiskBDB.CacheSize.NumberCaches);
		if( ret != 0 )
		{
			fprintf(stderr, "Error configuring disk cache: %s\n",
			db_strerror(ret));
		}
		
		ret = m_diskDbEnv->set_lk_detect( DB_LOCK_MINWRITE );
		if( ret != 0 )
		{
			fprintf(stderr, "Error setting lk_detect to DB_LOCK_MINWRITE for disk: %s\n",
			db_strerror(ret));
		}

		ret = 0;
		if( cp.DiskBDB.MaxLockers > 0 )
			ret = m_diskDbEnv->set_lk_max_lockers( cp.DiskBDB.MaxLockers );

		if( cp.DiskBDB.MaxLocks > 0 )
			ret |= m_diskDbEnv->set_lk_max_locks( cp.DiskBDB.MaxLocks );

		if( cp.DiskBDB.MaxLockObjects > 0 )
			ret |= m_diskDbEnv->set_lk_max_objects( cp.DiskBDB.MaxLockObjects );

		if( ret != 0 )
		{
			fprintf(stderr, "Error setting max lockers, locks or objects for disk: %s\n",
			db_strerror(ret));
		}


		ret = 0;
		if( cp.DiskBDB.MutexIncrement > 0 )
			ret = m_diskDbEnv->mutex_set_increment( cp.DiskBDB.MutexIncrement );

		if( cp.DiskBDB.MaxMutexes > 0 )
			ret |= m_diskDbEnv->mutex_set_max( cp.DiskBDB.MaxMutexes );

		if( ret != 0 )
		{
			fprintf(stderr, "Error configuring mutexes for disk: %s\n",
			db_strerror(ret));
		}

		ret = m_diskDbEnv->open( cp.DiskBDB.HomeDirectory.c_str() , open_flags, 0 ); 
		if( ret != 0 )
		{
			fprintf(stderr, "Error opening environment for disk: %s\n",
			db_strerror(ret));
		}
	}
	catch(DbException &e) {
		printf ("Error Setting up disk environment %s\n",e.what());
		return (-1);
	}  

	return ret;
}

int TCache::Create(const Vdt *dzName)
{	
	if( m_shutdownFlag > 0 )
	{
		m_logStream->print( "Error. Could not Create Datazone because the system has been shutdown\n" );
		return TC_SYSTEM_SHUTDOWN;
	}

	if( m_DiskEnabled )
		return m_dzManager->Create( *dzName );
	else
		return 0;
}

#ifdef V_CACHE_MANAGER_NEW_GET__
//*
int TCache::GetFromCache( const Vdt *dzName, const Vdt &Key, Vdt *Value, const int& partition_ID )
{
	ContextStructure* c_struct;
	m_contextManager->GetFreeContext( c_struct );

	//initiliaze MD strcut
	MDStructHist struct_hist;
	struct_hist.c_struct = c_struct;

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );

	int cache_flags = m_cachingAlgorithm->GetFlags();

	struct_hist.avgQValue = m_statsManager->GetAvgQ( partition_ID );
	
	// Get L value and use only the fractional part (i.e don't count the aging value)
	//double L_value = m_statsManager->GetL( partition_ID );
	struct_hist.LValue = m_statsManager->GetL( partition_ID );
	struct_hist.lastEvictedAgeValue = m_statsManager->GetLastEvictedAgeValue( partition_ID );

	int ret = -1;
	int counter = 0;

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	cKey.setValues( *dzName, Key );
	cValue.setDataRecValues( *Value );	

	int debug_int = *(int*) Key.get_data();

	// Check if the key is in the cache.
	ret = m_cacheDB->GetDataRec( cKey, cValue, partition_ID );

	// If the object was found in cache
	if( ret == 0 )
	{
		stats_update.NumCacheHits = 1;
		stats_update.NumBytesAccessedTotal += cValue.getDataRecSize();
		stats_update.NumBytesCacheHit += cValue.getDataRecSize();

		Value->set_size( cValue.getDataRecSize() );

		if( m_lock->acquireFail( *dzName, Key ) == 0 )
		{
			//Get MD data
			ret = GetHistoryMD( Key, Value, dzName, &struct_hist, partition_ID, TC_FLAG_MAINTAIN_MD );

			// If the old Age Value was different we should update anyway
			if( struct_hist.oldQValue.getInt() == struct_hist.newQValue.getInt() )
			{
				if( m_cachingAlgorithm->NoNeedToUpdateMetadata( struct_hist.oldQValue.getFraction(), struct_hist.newQValue.getFraction(), struct_hist.avgQValue,
					struct_hist.lastUpdateTimestamp, struct_hist.currTimestamp, m_CONST_updateDelta ) == true )
				{
					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
			}
			
			//update MD
			ret = UpdateMD( Key, Value, dzName, &struct_hist, partition_ID, TC_CACHE_KEY_NOT_ADMITTED, stats_update );
			if( ret != 0)
				m_logStream->print( "Error, TCache::UpdateMD. Could not update MD \n" );

			m_lock->release( *dzName, Key );	
			ret = 0;	
		}
	}

	m_contextManager->ReleaseContext( c_struct );
	UpdateStats( stats_update, partition_ID );
	return ret;
}

int TCache::GetFromDisk( const Vdt *dzName, const Vdt &Key, Vdt *Value, const int& partition_ID )
{
	ContextStructure* c_struct;
	m_contextManager->GetFreeContext( c_struct );

	//initiliaze MD strcut
	MDStructHist struct_hist;
	struct_hist.c_struct = c_struct;

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );
	
	//struct_hist.avgQValue = m_statsManager->GetAvgQ( partition_ID );
	
	// Get L value and use only the fractional part (i.e don't count the aging value)	
	struct_hist.LValue = m_statsManager->GetL( partition_ID );
	struct_hist.lastEvictedAgeValue = m_statsManager->GetLastEvictedAgeValue( partition_ID );

	long long freespace_bytes = m_statsManager->GetFreeSpaceBytes( partition_ID );

	int ret = -1;
	int counter = 0;

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	cKey.setValues( *dzName, Key );
	cValue.setDataRecValues( *Value );	
	
	int debug_int = *(int*) Key.get_data();

	// At this point we want to admit the key to Cache, so try to acquire a lock
	if( m_lock->acquireWait( *dzName, Key ) == 0 )
	{
		ret = m_diskDB->Get( *dzName, Key, Value, &stats_update );
		if( ret == 0 )
		{
			stats_update.NumBytesAccessedTotal += Value->get_size();
		}

		if( ret == 0 && m_MemoryEnabled )
		{
			
			int cache_flags = m_cachingAlgorithm->GetFlags();

			//if( (cache_flags & m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY) != 0 )
			if( (cache_flags & m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY) == m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY )
			{
				ret = GetHistoryMD( Key, Value, dzName, &struct_hist, partition_ID, TC_FLAG_MAINTAIN_MD );
			}
			else
			{
				ret = GetHistoryMD( Key, Value, dzName, &struct_hist, partition_ID, TC_FLAG_MAINTAIN_NO_MD );
			}

			if( (struct_hist.lastEvictedAgeValue >= m_agingMechanism->getAgingValue() - 2 ) ||
				(m_cachingAlgorithm->AdmitKey( struct_hist.newQValue.getFraction(), struct_hist.LValue ) == true) )
			{
				//update MD
				ret = UpdateMD( Key, Value, dzName, &struct_hist, partition_ID, TC_CACHE_KEY_ADMITTED, stats_update );
				if( ret != 0)
					m_logStream->print( "Error, TCache::UpdateMD. Could not update MD \n" );
			}
			else
			{
				if( (cache_flags & m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY) == m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY && ret == 0)
				{
					//update history MD
					struct_hist.newRecordSize *= -1;
					ret = UpdateMD( Key, Value, dzName, &struct_hist, partition_ID, TC_CACHE_KEY_NOT_ADMITTED, stats_update );
					if( ret != 0)
						m_logStream->print( "Error, TCache::UpdateMD. Could not update MD \n" );
				}

				//This code is to enable filling up the catch when there is free space, regarding the admission control
				/*
				if( Value->get_size() > freespace_bytes )
				{
					if( (cache_flags & m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY) == m_cachingAlgorithm->CA_FLAG_MAINTAIN_HISTORY && ret == 0)
					{
						//update history MD
						struct_hist.newRecordSize *= -1;
						ret = UpdateMD( Key, Value, dzName, &struct_hist, partition_ID, TC_CACHE_KEY_NOT_ADMITTED, stats_update );
						if( ret != 0)
							m_logStream->print( "Error, TCache::UpdateMD. Could not update MD \n" );
					}
					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
				
				//if there is space insert the new record into cache
				ret = UpdateMD( Key, Value, dzName, &struct_hist, partition_ID, TC_CACHE_KEY_ADMITTED, stats_update );
				if( ret != 0)
					m_logStream->print( "Error, TCache::UpdateMD. Could not update MD \n" );
				*/
			}
		}
		m_lock->release( *dzName, Key );
	}

	m_contextManager->ReleaseContext( c_struct );
	UpdateStats( stats_update, partition_ID );
	return ret;
}





//*/
#else
//*
int TCache::GetFromCache( const Vdt *dzName, const Vdt &Key, Vdt *Value, const int& partition_ID )
{
	ContextStructure* c_struct;
	m_contextManager->GetFreeContext( c_struct );

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );

	int cache_flags = m_cachingAlgorithm->GetFlags();
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter( &currTimestamp );	

	double avg_q_value = m_statsManager->GetAvgQ( partition_ID );
	
	// Get L value and use only the fractional part (i.e don't count the aging value)
	double L_value = m_statsManager->GetL( partition_ID );

	int ret = -1;
	int counter = 0;

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	cKey.setValues( *dzName, Key );
	cValue.setDataRecValues( *Value );	

	int debug_int = *(int*) Key.get_data();


	// Check if the key is in the cache.
	ret = m_cacheDB->GetDataRec( cKey, cValue, partition_ID );

	// If the object was found in cache
	if( ret == 0 )
	{
		stats_update.NumCacheHits = 1;
		stats_update.NumBytesAccessedTotal += cValue.getDataRecSize();
		stats_update.NumBytesCacheHit += cValue.getDataRecSize();

		Value->set_size( cValue.getDataRecSize() );

		if( m_lock->acquireFail( *dzName, Key ) == 0 )
		{
			ret = m_cacheDB->GetMDLookup( cKey, cValue, partition_ID );
			if( ret != 0 )
			{
				m_logStream->print( "TC::Get - Error. Could not retrieve md_lookup_rec from CacheDB. Maybe it was dirty?\n" );
				m_logStream->print( "TC::Get - Error. " + TCUtility::intToString(ret) + "\n" );
				m_lock->release( *dzName, Key );
				m_contextManager->ReleaseContext( c_struct );
				UpdateStats( stats_update, partition_ID );
				return 0;
			}

			TCQValue old_q_value = cValue.getQValue();
			char old_dirty_bit = cValue.getDirtyBit();

			QDBKey qKey( c_struct );
			QDBValue qValue( c_struct );

			qKey.setValues( old_dirty_bit, old_q_value, *dzName, Key );
			ret = m_qDB->Get( qKey, qValue, partition_ID );
			if( ret != 0 )
			{				
				// TODO: debugging code
				std::string debug_key = TCUtility::intToString( debug_int );
				std::string debug_msg = "TC::Get - Error. Could not find key ";
				debug_msg += *dzName->get_data();
				debug_msg += "-" + debug_key + " in Q_DB\n";
				std::cout<< "q value = " << old_q_value << std::endl;
				std::cout<< debug_msg;
				//m_logStream->print( debug_msg );
				m_lock->release( *dzName, Key );
				
				std::ofstream fout;
				fout.open( "QDB_Contents.txt", ios::out );
				//m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, std::cout );
				m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, fout );
				fout.close();

				//std::ofstream fout;
				fout.open( "CacheDB_Contents.txt", ios::out );
				//m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, std::cout );
				m_cacheDB->printContents( partition_ID, c_struct, fout );
				fout.close();



				m_contextManager->ReleaseContext( c_struct );
				//system( "pause" );
				UpdateStats( stats_update, partition_ID );
				return 0;
			}

			// Calculate new Q
			void* metadata = qValue.getMetadataPtr();
			m_cachingAlgorithm->UpdateRead( metadata );
			double new_q_value = m_cachingAlgorithm->calcQValue( metadata, L_value );
			new_q_value += m_agingMechanism->getAgingValue();

			if( m_cachingAlgorithm->NoNeedToUpdateMetadata( old_q_value, new_q_value, avg_q_value,
					qValue.getUpdateTimestamp(), currTimestamp, m_CONST_updateDelta ) == true )
			{
				m_lock->release( *dzName, Key );
				m_contextManager->ReleaseContext( c_struct );
				UpdateStats( stats_update, partition_ID );
				return 0;
			}

			// Insert the new MD lookup rec in CacheDB with new Q
			cValue.setMDValues( old_dirty_bit, new_q_value );
			ret = m_cacheDB->InsertMDLookup( cKey, cValue, partition_ID );
			if( ret != 0 )
			{
				if( ret == TC_FAILED_MEM_FULL )
				{
					ret = EvictAndInsertMDLookup( cKey, cValue, partition_ID, stats_update );					
					if( ret != 0 )
						m_logStream->print( "Error. Could not insert MDLookup even after trying to evict\n" );
					else
						m_logStream->print( "Error1. EvictAndInsertMDLookup failed in QDB\n" );
				}
				else
					m_logStream->print( "Error2. failed distinct than TC_FAILED_MEM_FULL in CacheMD\n" );
				
				
				// if eviction fails or Insert was not an out of memory error, release and return
				if( ret != 0 )
				{
					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
			}

			// Insert new QDB record with new Q (Retries on deadlock)
			qKey.setQValue( new_q_value );
			qValue.setValues( qValue.getRecordSize(), currTimestamp, m_cachingAlgorithm->GetDataSize() );

			ret = m_qDB->Insert( qKey, qValue, partition_ID );
			if( ret != 0 )
			{			
				// Insert failed because memory was full. Try to evict records
				//  to make room for the insert.
				if( ret == TC_FAILED_MEM_FULL )
				{
					ret = EvictAndInsertQDBRec( qKey, qValue, partition_ID, stats_update );
					if( ret != 0 )
					{
						m_logStream->print( "Error. Could not insert QDBRec even after trying to evict\n" );
					}
					else
						m_logStream->print( "Error1. EvictAndInsertQDBRec failed in QDB\n" );
				}
				else
						m_logStream->print( "Error2. failed distinct than TC_FAILED_MEM_FULL in QDB\n" );
			}

			// Insert new Q to QDB failed, so we try to revert to the old record
			if( ret != 0 )
			{
				cValue.setMDValues( old_dirty_bit, old_q_value );
				ret = m_cacheDB->InsertMDLookup( cKey, cValue, partition_ID );
				if( ret == 0 )
				{
					m_lock->release( *dzName, Key );
					m_logStream->print( "TC::Get - Had to revert to old Q value for MD Lookup\n" );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
				else
				{
					// Couldn't revert to the old record, so removing record from Cache
					m_logStream->print( "TC::Get - Error. Could not re-insert old md_lookup_rec.\n" );

					// Delete everything if it was clean
					if( old_dirty_bit == TC_CLEAN_FLAG )
					{
						if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
							m_logStream->print( "TC::Get - Error. Could not delete old Data Record from CacheDB\n" );
						else
							stats_update.cleanBytes -= Value->get_size();

						qKey.setQValue( old_q_value );
						if( m_qDB->Delete( qKey, partition_ID ) != 0 )
							m_logStream->print( "TC::Get - Error. Could not delete old record from QDB\n" );
					}

					// Otherwise, if it is dirty, just delete the MDLookup
					if( m_cacheDB->DeleteMDLookup( cKey, partition_ID ) != 0 )
						m_logStream->print( "TC::Get - Error. Could not delete old MD lookup from CacheDB\n" );

					m_lock->release( *dzName, Key );

					stats_update.numQ--;
					stats_update.totalQ -= old_q_value;

					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;				
				}
			}

			// Delete the old Q from QDB (only if q has changed. otherwise it'll delete the
			//  newly inserted record)
			if( old_q_value != new_q_value )
			{
				qKey.setQValue( old_q_value );

				ret = m_qDB->Delete( qKey, partition_ID );
				if( ret != 0 )
				{
					m_logStream->print( "TC::Get - Error. Could not delete oldQ value from QDB\n" );
				}
			}

			m_lock->release( *dzName, Key );	

			stats_update.totalQ += (new_q_value - old_q_value);		
		}

		// Could not acquire lock, so just return the value data and skip updating the metadata
		m_contextManager->ReleaseContext( c_struct );
		UpdateStats( stats_update, partition_ID );
		return 0;
	}

	m_contextManager->ReleaseContext( c_struct );
	UpdateStats( stats_update, partition_ID );
	return ret;
}

int TCache::GetFromDisk( const Vdt *dzName, const Vdt &Key, Vdt *Value, const int& partition_ID )
{
	ContextStructure* c_struct;
	m_contextManager->GetFreeContext( c_struct );

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );
	
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter( &currTimestamp );	

	double avg_q_value = m_statsManager->GetAvgQ( partition_ID );
	
	// Get L value and use only the fractional part (i.e don't count the aging value)
	double L_value = m_statsManager->GetL( partition_ID );

	long long freespace_bytes = m_statsManager->GetFreeSpaceBytes( partition_ID );

	int ret = -1;
	int counter = 0;

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	cKey.setValues( *dzName, Key );
	cValue.setDataRecValues( *Value );	

	int debug_int = *(int*) Key.get_data();

	// At this point we want to admit the key to Cache, so try to acquire a lock
	if( m_lock->acquireWait( *dzName, Key ) == 0 )
	{
		ret = m_diskDB->Get( *dzName, Key, Value, &stats_update );
		if( ret == 0 )
		{
			stats_update.NumBytesAccessedTotal += Value->get_size();
		}

		if( ret == 0 && m_MemoryEnabled )
		{
			int cache_flags = m_cachingAlgorithm->GetFlags();

			// Calculate new Q
				
			// Check the size of metadata buffer
			int metadata_size = m_cachingAlgorithm->GetDataSize();

			QDBKey qKey( c_struct );
			QDBValue qValue( c_struct );

			if( qValue.checkMetadataSize( metadata_size ) != 0 )
				m_logStream->print( "TC::Get - Error. Could not allocate memory for metadata buffer\n" );

			void* metadata = qValue.getMetadataPtr();
			m_cachingAlgorithm->GenerateMetadata( *dzName, Key, Value, metadata, metadata_size );

			double new_q_value = m_cachingAlgorithm->calcQValue( metadata, L_value );
			new_q_value += m_agingMechanism->getAgingValue();

			//This code is to enable filling up the catch when there is free space, regarding the admission control
			/*
			if( Value->get_size() > freespace_bytes )
			{
				if( m_cachingAlgorithm->AdmitKey( new_q_value, L_value ) == false )
				{
					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					// TODO: updateStats
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
			}
			*/
			if( m_cachingAlgorithm->AdmitKey( new_q_value, L_value ) == false )
			{
				m_lock->release( *dzName, Key );
				m_contextManager->ReleaseContext( c_struct );
				// TODO: updateStats
				UpdateStats( stats_update, partition_ID );
				return 0;
			}

			// Insert the data from disk to CacheDB
			cValue.setDataRecValues( *Value );
			ret = m_cacheDB->InsertDataRec( cKey, cValue, partition_ID );
			if( ret != 0 )
			{
				if( ret == TC_FAILED_MEM_FULL )
				{
					ret = EvictAndInsertDataRec( cKey, cValue, partition_ID, stats_update );
					if( ret != 0 )
					{
						m_logStream->print( "Error. Could not insert DataRec even after trying to evict\n" );
					}						
				}
				
				// if eviction fails or Insert was not an out of memory error, release and return
				if( ret != 0 )
				{
					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
			}


			// Insert the new MD lookup rec in CacheDB with new Q
			char dirty_bit = TC_CLEAN_FLAG;
			cValue.setMDValues( dirty_bit, new_q_value );
			ret = m_cacheDB->InsertMDLookup( cKey, cValue, partition_ID );
			if( ret != 0 )
			{
				if( ret == TC_FAILED_MEM_FULL )
				{
					ret = EvictAndInsertMDLookup( cKey, cValue, partition_ID, stats_update );
					if( ret != 0 )
					{
						m_logStream->print( "Error. Could not insert MDLookup even after trying to evict\n" );
					}						
				}
				
				// if eviction fails or Insert was not an out of memory error, release and return
				if( ret != 0 )
				{
					if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
						m_logStream->print( "TC::Get - Error. Could not remove Data rec from CacheDB as part of cleanup\n" );

					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
			}
	
			// Insert new QDB record with new Q
			qKey.setValues( dirty_bit, new_q_value, *dzName, Key );
			qValue.setValues( Value->get_size(), currTimestamp, m_cachingAlgorithm->GetDataSize() );

			ret = m_qDB->Insert( qKey, qValue, partition_ID );
			if( ret != 0 )
			{			
				// Insert failed because memory was full. Try to evict records
				//  to make room for the insert.
				if( ret == TC_FAILED_MEM_FULL )
				{
					ret = EvictAndInsertQDBRec( qKey, qValue, partition_ID, stats_update );
					if( ret != 0 )
					{
						m_logStream->print( "Error. Could not insert QDBRec even after trying to evict\n" );
					}
				}

				if( ret != 0 )
				{
					if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
						m_logStream->print( "TC::Get - Error. Could not remove Data rec from CacheDB as part of cleanup\n" );

					if( m_cacheDB->DeleteMDLookup( cKey, partition_ID ) != 0 )
						m_logStream->print( "TC::Get - Error. Could not remove MDLookup rec from CacheDB as part of cleanup\n" );

					m_lock->release( *dzName, Key );
					m_contextManager->ReleaseContext( c_struct );
					UpdateStats( stats_update, partition_ID );
					return 0;
				}
			}

			m_lock->release( *dzName, Key );
			

			stats_update.totalQ += new_q_value;
			stats_update.numQ++;
			stats_update.cleanBytes += Value->get_size();
			stats_update.freespaceBytes -= Value->get_size();

			UpdateStats( stats_update, partition_ID );

			// Failed to acquire the lock, so just return
			m_contextManager->ReleaseContext( c_struct );
			return 0;
		}
		else
		{
			// Object not found on the disk
			m_lock->release( *dzName, Key );
		}
	}

	m_contextManager->ReleaseContext( c_struct );
	UpdateStats( stats_update, partition_ID );
	return ret;
}


//*/
#endif

int TCache::Get( const Vdt *dzName, const Vdt &Key, Vdt *Value )
{
	int ret = -1;

	if( m_shutdownFlag > 0 )
	{
		m_logStream->print( "Error. Could not Get because the system has been shutdown\n" );
		return TC_SYSTEM_SHUTDOWN;
	}

	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );
	stats_update.NumRequests = 1;
	stats_update.NumGets = 1;

	int partition_ID = TCUtility::bernsteinHash( *dzName, Key, m_numPartitions );

	if( m_MemoryEnabled )
		ret = GetFromCache( dzName, Key, Value, partition_ID );

	if( ret != 0 && m_DiskEnabled )
		ret = GetFromDisk( dzName, Key, Value, partition_ID );

	if( ret == 0 )
	{
		stats_update.NumRequestsKeyFound = 1;
	}
	
	UpdateStats( stats_update, partition_ID );

	return ret;
}


int TCache::GetHistoryMD( const Vdt &Key, const Vdt *Value, const Vdt *dzName, MDStructHist *md_struct, const int& partition_ID, const int& MAINTAIN_MD )
{
	int ret = -1;

	ContextStructure* c_struct;
	c_struct = md_struct->c_struct;

	int cache_flags = m_cachingAlgorithm->GetFlags();
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter( &currTimestamp );	
	md_struct->currTimestamp = currTimestamp;
	
	int counter = 0;

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	cKey.setValues( *dzName, Key );
	cValue.setDataRecValues( *Value );	

	int debug_int = *(int*) Key.get_data();

	if( MAINTAIN_MD == TC_FLAG_MAINTAIN_MD )
		ret = m_cacheDB->GetMDLookup( cKey, cValue, partition_ID );

	if( ret != 0 )
	{
		if( MAINTAIN_MD == TC_FLAG_MAINTAIN_MD )
		{
			m_logStream->print( "TC::GetHistoryMD - Error. MD not found \n" );
			//m_logStream->print( "TC::Get - Error. Could not retrieve md_lookup_rec from CacheDB. Maybe it was dirty?\n" );
			//m_logStream->print( "TC::Get - Error. " + TCUtility::intToString(ret) + "\n" );
		}
		
		md_struct->recordSize = 0;
		md_struct->newRecordSize = Value->get_size();
		//Calculate Q value
			
		// Check the size of metadata buffer
		int metadata_size = m_cachingAlgorithm->GetDataSize();

		QDBKey qKey( c_struct );
		QDBValue qValue( c_struct );

		if( qValue.checkMetadataSize( metadata_size ) != 0 )
			m_logStream->print( "TC::Get - Error. Could not allocate memory for metadata buffer\n" );

		void* metadata = qValue.getMetadataPtr();
		m_cachingAlgorithm->GenerateMetadata( *dzName, Key, Value, metadata, metadata_size );

		double new_q_value = m_cachingAlgorithm->calcQValue( metadata, md_struct->LValue );
		//new_q_value += m_agingMechanism->getAgingValue();

		//md_struct->oldQValue = 0.0;
		md_struct->oldQValue.setInt( 0 );
		md_struct->oldQValue.setFraction( 0.0 );

		md_struct->newQValue.setFraction( new_q_value );
		md_struct->newQValue.setInt( m_agingMechanism->getAgingValue() );

		md_struct->oldDirtyBit = TC_CLEAN_FLAG;
		md_struct->recordSize = 0;
		md_struct->newRecordSize = Value->get_size();

		ret = 0;
	}
	else
	{

		TCQValue old_q_value = cValue.getQValue();
		char old_dirty_bit = cValue.getDirtyBit();

		QDBKey qKey( c_struct );
		QDBValue qValue( c_struct );

		qKey.setValues( old_dirty_bit, old_q_value, *dzName, Key );
		ret = m_qDB->Get( qKey, qValue, partition_ID );


		if( ret != 0 )
		{				
			// TODO: debugging code
			std::string debug_key = TCUtility::intToString( debug_int );
			std::string debug_msg = "TC::Get - Error. Could not find key ";
			debug_msg += *dzName->get_data();
			debug_msg += "-" + debug_key + " in Q_DB\n";
			std::cout<< "q value = " << old_q_value << std::endl;
			std::cout<< debug_msg;
			//m_logStream->print( debug_msg );
			m_lock->release( *dzName, Key );
			
			std::ofstream fout;
			fout.open( "QDB_Contents.txt", ios::out );
			//m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, std::cout );
			m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, fout );
			fout.close();

			//std::ofstream fout;
			fout.open( "CacheDB_Contents.txt", ios::out );
			//m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, std::cout );
			m_cacheDB->printContents( partition_ID, c_struct, fout );
			fout.close();

			//m_contextManager->ReleaseContext( c_struct );
			//system( "pause" );
			//UpdateStats( stats_update, partition_ID );
			return -1;
		}

		// Calculate new Q
		void* metadata = qValue.getMetadataPtr();
		m_cachingAlgorithm->UpdateRead( metadata );
		double new_q_value = m_cachingAlgorithm->calcQValue( metadata, md_struct->LValue );
		//new_q_value += m_agingMechanism->getAgingValue();

		md_struct->oldDirtyBit = old_dirty_bit;
		md_struct->oldQValue = old_q_value;
		
		//md_struct->newQValue = new_q_value;
		md_struct->newQValue.setFraction( new_q_value );
		md_struct->newQValue.setInt( m_agingMechanism->getAgingValue() );

		md_struct->recordSize = qValue.getRecordSize();
		md_struct->newRecordSize = Value->get_size();
		md_struct->lastUpdateTimestamp = qValue.getUpdateTimestamp();

		ret = 0;
	}

	return ret;
}

int TCache::UpdateMD( const Vdt &Key, const Vdt *Value, const Vdt *dzName, MDStructHist *md_struct, const int& partition_ID, const int& ADMITED_KEY_FLAG, stats& stats_update )
{
	int ret = -1;

	ContextStructure* c_struct;
	c_struct = md_struct->c_struct;

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	cKey.setValues( *dzName, Key );

	if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_ADMITTED )
	{	
		cValue.setDataRecValues( *Value );	

		int debug_int = *(int*) Key.get_data();

		// Insert the data from disk to CacheDB
		cValue.setDataRecValues( *Value );
		ret = m_cacheDB->InsertDataRec( cKey, cValue, partition_ID );
		if( ret != 0 )
		{
			if( ret == TC_FAILED_MEM_FULL )
			{
				ret = EvictAndInsertDataRec( cKey, cValue, partition_ID, stats_update );
				if( ret != 0 )
				{
					m_logStream->print( "Error. Could not insert DataRec even after trying to evict\n" );
				}						
			}
			
			// if eviction fails or Insert was not an out of memory error, release and return
			if( ret != 0 )
			{
				//m_contextManager->ReleaseContext( c_struct );
				//UpdateStats( stats_update, partition_ID );
				return 0;
			}
		}
	}

	// Insert the new MD lookup rec in CacheDB with new Q
	cValue.setMDValues( md_struct->oldDirtyBit, md_struct->newQValue );
	ret = m_cacheDB->InsertMDLookup( cKey, cValue, partition_ID );
	if( ret != 0 )
	{
		if( ret == TC_FAILED_MEM_FULL )
		{
			ret = EvictAndInsertMDLookup( cKey, cValue, partition_ID, stats_update );					
			if( ret != 0 )
				m_logStream->print( "Error. Could not insert MDLookup even after trying to evict\n" );
			else
				m_logStream->print( "Error1. EvictAndInsertMDLookup failed in QDB\n" );
		}
		else
			m_logStream->print( "Error2. failed distinct than TC_FAILED_MEM_FULL in CacheMD\n" );
		
		
		// if eviction fails or Insert was not an out of memory error, release and return
		if( ret != 0 )
		{
			if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_ADMITTED )
			{
				if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
					m_logStream->print( "TC::Get - Error. Could not delete old Data Record from CacheDB\n" );
			}
			//m_contextManager->ReleaseContext( c_struct );
			//UpdateStats( stats_update, partition_ID );
			return 0;
		}
	}

	QDBKey qKey( c_struct );
	QDBValue qValue( c_struct );

	// Insert new QDB record with new Q (Retries on deadlock)
	qKey.setValues( md_struct->oldDirtyBit, md_struct->newQValue, *dzName, Key );
	qValue.setValues( md_struct->newRecordSize, md_struct->currTimestamp, m_cachingAlgorithm->GetDataSize() );

	ret = m_qDB->Insert( qKey, qValue, partition_ID );
	if( ret != 0 )
	{			
		// Insert failed because memory was full. Try to evict records
		//  to make room for the insert.
		if( ret == TC_FAILED_MEM_FULL )
		{
			ret = EvictAndInsertQDBRec( qKey, qValue, partition_ID, stats_update );
			if( ret != 0 )
			{
				m_logStream->print( "Error. Could not insert QDBRec even after trying to evict\n" );
			}
			else
				m_logStream->print( "Error1. EvictAndInsertQDBRec failed in QDB\n" );
		}
		else
				m_logStream->print( "Error2. failed distinct than TC_FAILED_MEM_FULL in QDB\n" );
	}

	// Insert new Q to QDB failed, so we try to revert to the old record
	if( ret != 0 )
	{
		if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_ADMITTED && md_struct->recordSize >= 0 )
		{
			if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
				m_logStream->print( "TC::Get - Error. Could not remove Data rec from CacheDB as part of cleanup\n" );

			if( m_cacheDB->DeleteMDLookup( cKey, partition_ID ) != 0 )
				m_logStream->print( "TC::Get - Error. Could not remove MDLookup rec from CacheDB as part of cleanup\n" );

			//m_contextManager->ReleaseContext( c_struct );
			//UpdateStats( stats_update, partition_ID );
			return 0;
		}

		cValue.setMDValues( md_struct->oldDirtyBit, md_struct->oldQValue );
		ret = m_cacheDB->InsertMDLookup( cKey, cValue, partition_ID );
		if( ret == 0 )
		{
			if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_ADMITTED )
			{
				if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
				m_logStream->print( "TC::Get - Error. Could not remove Data rec from CacheDB as part of cleanup\n" );
			}

			m_logStream->print( "TC::Get - Had to revert to old Q value for MD Lookup\n" );
			//m_contextManager->ReleaseContext( c_struct );
			//UpdateStats( stats_update, partition_ID );
			return 0;
		}
		else
		{
			// Couldn't revert to the old record, so removing record from Cache
			m_logStream->print( "TC::Get - Error. Could not re-insert old md_lookup_rec.\n" );
			// Delete everything if it was clean

			/*
			if( md_struct->oldDirtyBit == TC_CLEAN_FLAG && md_struct->newRecordSize >= 0 )
			{
				if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
					m_logStream->print( "TC::Get - Error. Could not delete old Data Record from CacheDB\n" );
				else
					stats_update.cleanBytes -= Value->get_size();

				qKey.setQValue( md_struct->oldQValue );		
				if( m_qDB->Delete( qKey, partition_ID ) != 0 )
					m_logStream->print( "TC::Get - Error. Could not delete old record from QDB\n" );
			}
			if( md_struct->oldDirtyBit == TC_CLEAN_FLAG && md_struct->newRecordSize < 0 )
			{

				qKey.setQValue( md_struct->oldQValue );
				if( m_qDB->Delete( qKey, partition_ID ) != 0 )
					m_logStream->print( "TC::Get - Error. Could not delete old record from QDB\n" );
			}
			//*/

			//*
			if( md_struct->oldDirtyBit == TC_CLEAN_FLAG )
			{
				if( md_struct->newRecordSize >= 0 )
				{
					if( m_cacheDB->DeleteDataRec( cKey, partition_ID ) != 0 )
						m_logStream->print( "TC::Get - Error. Could not delete old Data Record from CacheDB\n" );
					else
						stats_update.cleanBytes -= Value->get_size();
				}

				qKey.setQValue( md_struct->oldQValue );
				if( m_qDB->Delete( qKey, partition_ID ) != 0 )
					m_logStream->print( "TC::Get - Error. Could not delete old record from QDB\n" );
			}
			//*/

			// Otherwise, if it is dirty, just delete the MDLookup
			if( m_cacheDB->DeleteMDLookup( cKey, partition_ID ) != 0 )
				m_logStream->print( "TC::Get - Error. Could not delete old MD lookup from CacheDB\n" );

			if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_NOT_ADMITTED && md_struct->newRecordSize >= 0 )
			{
				stats_update.numQ--;
				stats_update.totalQ -= md_struct->oldQValue.getFraction();
			}

			//m_contextManager->ReleaseContext( c_struct );
			//UpdateStats( stats_update, partition_ID );
			return 0;				
		}
	}

	// Delete the old Q from QDB (only if q has changed. otherwise it'll delete the
	//  newly inserted record)
	if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_NOT_ADMITTED )
	{
		if( md_struct->oldQValue != md_struct->newQValue )
		{
			qKey.setQValue( md_struct->oldQValue );

			ret = m_qDB->Delete( qKey, partition_ID );
			if( ret != 0 )
			{
				m_logStream->print( "TC::Get - Error. Could not delete oldQ value from QDB\n" );
			}
		}
	}

	//check this condition
	if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_NOT_ADMITTED && md_struct->newRecordSize >= 0 )
		stats_update.totalQ += (md_struct->newQValue - md_struct->oldQValue).getFraction();	

	if(	ADMITED_KEY_FLAG ==	TC_CACHE_KEY_ADMITTED )
	{
		stats_update.totalQ += md_struct->newQValue.getFraction();
		stats_update.numQ++;
		stats_update.cleanBytes += md_struct->newRecordSize;
		stats_update.freespaceBytes -= md_struct->newRecordSize;
	}

	//m_contextManager->ReleaseContext( c_struct );
	//UpdateStats( stats_update, partition_ID );
	return ret;
}


int TCache::Insert( const Vdt *dzName, const Vdt &Key, const Vdt &Value, const double& cost )
{
	if( m_shutdownFlag > 0 )
	{
		m_logStream->print( "Error. Could not Insert because the system has been shutdown\n" );
		return TC_SYSTEM_SHUTDOWN;
	}

	double new_cost = cost;
	if( new_cost < 1.0 )
		new_cost = 1.0;

	int ret=-1;
	int partitionID= TCUtility::bernsteinHash(*dzName,Key,m_numPartitions);
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );
	
	stats_update.NumInserts=1;
	stats_update.NumRequests=1;

	if (!m_DiskEnabled &&m_MemoryEnabled)
	{
		UpdateStats(stats_update,partitionID);
		return SynchronousInsert(*dzName,Key,Value,partitionID,true,false, new_cost);
	}

	if (m_dzManager->Exists(*dzName) !=0)
	{
		m_logStream->print( "TC::Insert failed because Datazone does not exist\n" );
		return TC_DATAZONE_NOT_FOUND;
	}

	UpdateStats(stats_update,partitionID);

	// Disk only, insert goes directly to disk component
	if ( m_DiskEnabled && !m_MemoryEnabled)
		return m_diskDB->Put(*dzName,Key,Value);
	
	
	if(m_statsManager->GetPartitionMode(partitionID)) // If Asynchrnous
	{
		ret=AsynchronousInsert(*dzName,Key,Value);
		if(ret==TC_DIRTY_CLEAN_RATIO_FAILURE)
			ret=SynchronousInsert(*dzName,Key,Value,partitionID,true);
		else if(ret!=0 && m_statsManager->GetNumSynchronousInsert() < m_maxSynchronousInserts)
			ret=SynchronousInsert(*dzName,Key,Value,partitionID,false);
	}
	else
		ret=SynchronousInsert(*dzName,Key,Value,partitionID,true);
	return ret;
}


int TCache::Delete( const Vdt *dzName, const Vdt &Key )
{
	int ret = -1, retDiskDB = -1;

	if( m_shutdownFlag > 0 )
	{
		m_logStream->print( "TC::Delete - Error. Could not Delete because the system has been shutdown\n" );
		return TC_SYSTEM_SHUTDOWN;
	}
	
	ContextStructure *c_struct;
	m_contextManager->GetFreeContext( c_struct );

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );

	int partition_ID = TCUtility::bernsteinHash( *dzName, Key, m_numPartitions );

	stats_update.NumRequests = 1;
	stats_update.NumDeletes = 1;

	CacheKey cKey( c_struct );
	cKey.setValues( *dzName, Key );

	if( m_lock->acquireWait( *dzName, Key ) == 0 )
	{		
		//delete from Disk
		if( m_DiskEnabled )
		{
			retDiskDB = m_diskDB->Delete( *dzName, Key,&stats_update );
		}

		if( !m_MemoryEnabled )
		{
			m_lock->release( *dzName, Key );
			m_contextManager->ReleaseContext( c_struct );
			UpdateStats( stats_update, partition_ID );
			return retDiskDB;
		}
		
		//delete Data record from Cache
		ret = m_cacheDB->DeleteDataRec( cKey, partition_ID );
		//if couldn't delete the record, then return Error Code
		if( ret != 0 )
		{			
			m_lock->release( *dzName, Key );
			m_contextManager->ReleaseContext( c_struct );
			UpdateStats( stats_update, partition_ID );
			//if not found in Cache, return success. Record was deleted from Disk
			if( retDiskDB == DB_NOTFOUND && ret == DB_NOTFOUND )
			{
				//m_logStream->print( "TC::Delete. Key not Found in Cache \n" );
				return ret;
			}
			else if( retDiskDB == 0 && ret == DB_NOTFOUND )
				return 0;
			else if( retDiskDB == 0 && ret != DB_NOTFOUND ) 
				m_logStream->print( "TC::Delete - Error. Could NOT delete Data record from Cache \n" );

			return ret;
		}

		//get Metadata from Cache
		CacheValue MDValue( c_struct );
		ret = m_cacheDB->GetMDLookup( cKey, MDValue, partition_ID );
		//If MDLookup object not found return Error Code. FATAL ERROR NOT DESIRERABLE
		if( ret != 0 )
		{
			//Error. cannot create the QDKey without the oldQValue and dirtyBit. 
			m_logStream->print( "TC::Delete - Error. Could NOT find MD record from Cache, unable to proceed delete operation \n" );
			
			m_lock->release( *dzName, Key );
			m_contextManager->ReleaseContext( c_struct );
			UpdateStats( stats_update, partition_ID );
			return TC_DELETE_MD_NOT_FOUND;
		}

		TCQValue oldQvalue = MDValue.getQValue();
		char dirtyBit = MDValue.getDirtyBit();
		
		//delete Metadata from Cache
		ret = m_cacheDB->DeleteMDLookup( cKey, partition_ID );
		if( ret != 0 )
			m_logStream->print( "TC::Delete - Error. Could NOT delete MDLookup record from Cache \n" );

		//Construct QDB Key
		QDBKey qKey( c_struct );
		QDBValue qValue( c_struct );
		qKey.setValues( dirtyBit, oldQvalue, *dzName, Key );
		
		//Get QDB Record
		ret = m_qDB->Get(qKey, qValue, partition_ID );
		if( ret != 0 )
			m_logStream->print( "TC::Delete - Error. Could NOT retrieve QDB record \n" );

		//Delete QDB record
		ret = m_qDB->Delete( qKey, partition_ID );
		if( ret != 0 )
			m_logStream->print( "TC::Delete - Error. Could NOT delete record from QDB \n" );
		else
		{
			if( dirtyBit == TC_CLEAN_FLAG )
				stats_update.cleanBytes -= qValue.getRecordSize();
			else
				stats_update.dirtyBytes -= qValue.getRecordSize();
		}

		m_lock->release( *dzName, Key );
		m_contextManager->ReleaseContext( c_struct );

		stats_update.numQ--;
		stats_update.totalQ -= oldQvalue.getFraction();
		UpdateStats( stats_update, partition_ID );
		return 0;
	}
	else
		m_logStream->print( "TC::Delete - Error. AcquireWait Failed \n" );
	
	m_contextManager->ReleaseContext( c_struct );
	UpdateStats( stats_update, partition_ID );
	return ret;
}

int TCache::DeleteAllKeysFromDZone( const Vdt* dzName )
{

	int ret = -1;

	if( m_shutdownFlag > 0 )
	{
		m_logStream->print( "TC::Delete - Error. Could not Delete because the system has been shutdown\n" );
		return TC_SYSTEM_SHUTDOWN;
	}

	/*
	*	Delete Keys from Disk - this part is performed by a separate thread.
	*	This part is handled by a separate thread
	*/
	m_truncateDZList.push_back( dzName );
	//Release the semaphore to avoid an infinite wait. 
	if (!ReleaseSemaphore(m_semaphoreDatazoneDelete,  // handle to semaphore
		1,                      // increase count by one
		NULL))            // not interested in previous count
	{
		printf("ReleaseSemaphore() failed, error: %d.\n", GetLastError());
		return -1;
	}	
	/*/
	/*
	*	Delete Keys from Disk - END
	*/

	/*
	*	Delete Keys from Cache
	*/
	//Delete all objects from DZone, looping range base on MD objects
	ret = DeleteCacheKeysByDZ( dzName, TC_DELDZKEYS_RANGE_MD );
	//Delete all objects from DZone, looping range base on Data_rec objects. Sanity run to clear out those records without a corresponding MD object
	ret = DeleteCacheKeysByDZ( dzName, TC_DELDZKEYS_RANGE_DATA );
	
	/*
	*	End of Cache DZone keys Deletion
	*/

	return ret;
}

int TCache::DeleteCacheKeysByDZ( const Vdt* dzName,	const int KEY_RANGE_FLAG )
{
	int ret = -1;
	bool cursorClosed = false;
	bool quitDeletion = false;
	bool lock_released = false;
	int deleteCount = 0;
	Dbc* cursorp = NULL;
	DbTxn* txn = NULL;
	string s_dzName, sintro_dzName, s_key;
	int Buff_Val_size = TC_USER_OBJECT_BUFFER_SIZE;
	//char *cBuff_Val = new char[Buff_Val_size]; //This is need just in case the resize mem block logic is used
	char cBuff_Val[TC_USER_OBJECT_BUFFER_SIZE];
	int rec_size;

	//sintro_dzName.insert(0, dzName->get_data(), dzName->get_size() );

	ContextStructure *c_struct;
	m_contextManager->GetFreeContext( c_struct );

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	
	QDBKey qdbKey( c_struct );
	QDBValue qdbValue( c_struct );

	CacheKey cKey( c_struct );
	CacheValue cValue( c_struct );
	
	int empty_char = 0;
	//Vdt dzNameEmpty( &empty_char, sizeof(empty_char) );
	Vdt keyEmpty( (char*)&empty_char, sizeof(empty_char) );
	Vdt vKey;
	Vdt vDZName;
	char* cKeyData;
	int cKeySize;
	char dirtybit;
	TCQValue qvalue;

	for(int i = 0; i < m_numPartitions; i++)
	{	
		quitDeletion = false; 
		memset( &stats_update, 0, sizeof(stats_update) );

		while( !quitDeletion )
		{
			//From here, include this code in a new method
			if( m_cacheDB->GetDBCursor( cursorp, txn, i, &stats_update ) != 0 )
				m_logStream->print( "Error. TCache::DeleteCacheKeysDZByRange. Couldn't open cursor on QDB\n" );

			Dbt dbtCKey, dbtCValue;
			
			if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_MD )
			{
				cKey.setValues( TC_CACHE_MD_LOOKUP_KEY_HEADER, *dzName, keyEmpty );

				dbtCValue.set_data( c_struct->md_lookup_value );
				dbtCValue.set_size( c_struct->md_lookup_value_size );
				dbtCValue.set_ulen( c_struct->md_lookup_value_size );
				dbtCValue.set_flags( DB_DBT_USERMEM );
			}
			else if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_DATA )
			{
				cKey.setValues( TC_CACHE_DATA_REC_KEY_HEADER, *dzName, keyEmpty );

				dbtCValue.set_data( cBuff_Val );
				dbtCValue.set_size( Buff_Val_size );
				dbtCValue.set_ulen( Buff_Val_size );
				dbtCValue.set_flags( DB_DBT_USERMEM );
			}

			cKey.getData( cKeyData, cKeySize );
			dbtCKey.set_data( cKeyData );
			dbtCKey.set_size( cKeySize );
			dbtCKey.set_ulen( cKeySize );
			dbtCKey.set_flags( DB_DBT_USERMEM );

			cursorClosed = false;		
			deleteCount = 0;
			try
			{
				/* BEBUG
				s_dzName.clear();
				//s_key.clear();
				s_dzName.insert( 0, cKey.getDzName().get_data(), cKey.getDzName().get_size() );
				//s_key.insert( 0, cKey.getKey().get_data(), cKey.getKey().get_size() );
				cout << "Cache Header Flag: " << cKey.getHeaderFlag() << "\n";
				cout << "DZone: " << s_dzName << "\n";
				cout<< TCUtility::intToString(*(int*)cKey.getKey().get_data()) << endl;
				*/

				//position the cursor at the beginning of clean records
				ret = cursorp->get( &dbtCKey, &dbtCValue, DB_SET_RANGE );
				cKey.setValues( dbtCKey.get_data(), dbtCKey.get_size() );

				/* DEBUG
				s_dzName.clear();
				//s_key.clear();
				s_dzName.insert( 0, cKey.getDzName().get_data(), cKey.getDzName().get_size() );
				//s_key.insert( 0, cKey.getKey().get_data(), cKey.getKey().get_size() );
				cout << "Cache Header Flag: " << cKey.getHeaderFlag() << "\n";
				cout << "DZone: " << s_dzName << "\n";
				cout<< TCUtility::intToString(*(int*)cKey.getKey().get_data()) << endl;
				//*/
				
				if( ret != 0 )
					quitDeletion = true;

			}
			catch( DbException& ex )
			{
				ret = ex.get_errno();
				//cout << ex.what() << endl;
				if( ret == DB_LOCK_DEADLOCK )
				{
					stats_update.NumDeadlocks++;
					m_logStream->print( "Error. TCache::DeleteCacheKeysDZByRange. Encountered deadLock while trying to Get DB_SET_RANGE. \n" );
				}
				else if( ret == DB_BUFFER_SMALL )
				{
					if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_DATA )
					{
						ret = 0;
						cKey.setValues( dbtCKey.get_data(), dbtCKey.get_size() );
						cValue.setDataRecValues( dbtCValue.get_data(), dbtCValue.get_size() );
					}
					else
					{
						//QuitDeletion process or resize MD Mem block???
					}

					m_logStream->print( "Error. TCache::DeleteCacheKeysDZByRange. Encountered small Buffer Size problem for value object in Cache. \n" );
					/* BEBUG
					cKey.setValues( dbtCKey.get_data(), dbtCKey.get_size() );
					s_dzName.clear();
					//s_key.clear();
					s_dzName.insert( 0, cKey.getDzName().get_data(), cKey.getDzName().get_size() );
					//s_key.insert( 0, cKey.getKey().get_data(), cKey.getKey().get_size() );
					cout << "Cache Header Flag: " << cKey.getHeaderFlag() << "\n";
					cout << "DZone: " << s_dzName << "\n";
					cout<< TCUtility::intToString(*(int*)cKey.getKey().get_data()) << endl; 
					//cout << "OID: " << s_key << "\n" ;
					*/
				}
				else
				{
					quitDeletion = true;
					m_logStream->print( ex.what() );
					m_logStream->print( "\n" );
				}

				if( ret != 0 )
				{
					cursorClosed = true;						
					cursorp->close();
					txn->abort();
					//add flag to retry in case of deadlocks
				}
			}

			if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_MD )
			{
				cValue.setMDValues( dbtCValue.get_data(), dbtCValue.get_size() );
				if( cKey.getHeaderFlag() != TC_CACHE_MD_LOOKUP_KEY_HEADER )
				{
					deleteCount = TC_NUM_CURSOR_EVICTIONS + 1;
					quitDeletion = true;
					m_logStream->print( "Error. TCache::DeleteCacheKeysDZByRange. Wrong Range" );
				}
			}
			else if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_DATA )
			{
				cValue.setDataRecValues( dbtCValue.get_data(), dbtCValue.get_size() );
				if( cKey.getHeaderFlag() != TC_CACHE_DATA_REC_KEY_HEADER )
				{
					deleteCount = TC_NUM_CURSOR_EVICTIONS + 1;
					quitDeletion = true;
					m_logStream->print( "Error. TCache::DeleteCacheKeysDZByRange. Wrong Range" );
				}
			}

			//while( deleteCount < TC_NUM_CURSOR_EVICTIONS && ret == 0 )
			while( deleteCount < 1 && ret == 0 )
			{
				rec_size = 0;
				vDZName = cKey.getDzName();				
				//check if we are located at the right DZone
				if( TCUtility::compare( *dzName, vDZName ) != 0 )
				{
					deleteCount = TC_NUM_CURSOR_EVICTIONS + 1;
					cursorClosed = true;
					quitDeletion = true;
					cursorp->close();
					txn->commit(0);
				}
				else
				{
					vKey = cKey.getKey();
					cout << "Cache Header Flag: " << cKey.getHeaderFlag() << "\n";
					cout<< TCUtility::intToString(*(int*)vKey.get_data()) << endl;
					dirtybit = cValue.getDirtyBit();
					qvalue = cValue.getQValue();
					if(m_lock->acquireWait( vDZName, vKey ) == 0)
					{
						lock_released = false;
						try
						{
							/*
							* If TC_DELDZKEYS_RANGE_MD, delete MD object pointed by cursor, 
							* Data_rec object and Qdb object should also be deleted in this if that follows
							*/
							if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_MD )
							{
								ret = cursorp->del( 0 );
								if( ret != 0 )
								{
									m_logStream->print( "TC::Delete - Error. Could NOT delete MDLookup record from Cache \n" );									
									cursorClosed = true;
									cursorp->close();
									txn->abort();
								}
								else
								{
									cursorClosed = true;
									cursorp->close();
									txn->commit(0);
								}

								ret = m_cacheDB->DeleteDataRec( cKey, i, &stats_update );
								if( ret != 0 )
								{			
									//if not found in Cache, return success. Record was deleted from Disk
									if( ret == DB_NOTFOUND )
									{
										m_logStream->print( "TC::DeleteAllKeysFromDZone. Key not Found in Cache \n" );
									}
									else
										m_logStream->print( "TC::DeleteAllKeysFromDZone - Error. Could NOT delete Data record from Cache \n" );
								}
							
								qdbKey.setValues( dirtybit, qvalue, vDZName, vKey );
								ret = m_qDB->Get( qdbKey, qdbValue, i, &stats_update );
								if( ret != 0 )
									m_logStream->print( "Error. Could not Get qdb record\n" );
								
								rec_size = qdbValue.getRecordSize();

								ret = m_qDB->Delete( qdbKey, i, &stats_update );
								if( ret != 0 )
									m_logStream->print( "Error. Could not evict qdb record\n" );
							}
							/*Important:
							* If TC_DELDZKEYS_RANGE_DATA, just delete Data_rec object pointed by cursor, 
							* MD object and Qdb object should not be deleted
							*/
							else if( KEY_RANGE_FLAG == TC_DELDZKEYS_RANGE_DATA )
							{
								if(!cursorClosed)
								{
									cursorClosed = true;
									cursorp->close();
									txn->commit(0);
								}
								
								ret = m_cacheDB->DeleteDataRec( cKey, i, &stats_update );
								if( ret != 0 )
								{			
									//if not found in Cache, return success. Record was deleted from Disk
									if( ret == DB_NOTFOUND )
									{
										m_logStream->print( "TC::DeleteAllKeysFromDZone. Key not Found in Cache \n" );
									}
									else
										m_logStream->print( "TC::DeleteAllKeysFromDZone - Error. Could NOT delete Data record from Cache \n" );
								}
								rec_size = cValue.getDataRecSize();
							}

							m_lock->release( vDZName, vKey );
							lock_released = true;

							deleteCount++;
							
							//update stats
							cout << "NumQ: " << TCUtility::intToString(stats_update.numQ) << endl;
							stats_update.numQ--;
							stats_update.totalQ -= qdbKey.getQValue().getFraction();
							if( qdbKey.getHeaderFlag() == TC_DIRTY_FLAG )
								stats_update.dirtyBytes -= rec_size;
							else if( qdbKey.getHeaderFlag() == TC_CLEAN_FLAG )
								stats_update.cleanBytes -= rec_size;

							if( !cursorClosed )
							{
								ret = cursorp->get( &dbtCKey, &dbtCValue, DB_NEXT );
								if( ret != 0)
									quitDeletion = true;
								else
								{
									cKey.setValues( dbtCKey.get_data(), dbtCKey.get_size() );
									cValue.setMDValues( dbtCValue.get_data(), dbtCValue.get_size() );
								}
							}
							

						}
						catch(DbException ex)
						{
							ret = ex.get_errno();
							if( ret == DB_LOCK_DEADLOCK )
							{
								stats_update.NumDeadlocks++;
								m_logStream->print( "Error. Encountered deadLock while trying to Get DB_SET_RANGE. \n" );
							}
							cursorClosed = true;
							cursorp->close();
							txn->abort();

							deleteCount = TC_NUM_CURSOR_EVICTIONS + 1;
						}
						if(!lock_released)
							m_lock->release( vDZName, vKey );
						
					}
				}			
			}

			if( !cursorClosed )
			{
				cursorClosed = true;
				cursorp->close();
				txn->commit(0);
			}
		}

		UpdateStats( stats_update, i );
	}

	//delete [] cBuff_Val;
	m_contextManager->ReleaseContext( c_struct );
	return ret;

}



int TCache::TruncateDZThread_W(const Vdt *dzName) 
{
	int ret = -1;

	/*
	int listSize = m_truncateDZList.size();
	if( listSize == 0 )
		return ret;

	truncDZ* trunc_struct;
	trunc_struct = (truncDZ*)m_truncateDZList.front();
	if( trunc_struct == NULL )
		return ret;

	TCache* vcm = (TCache*)trunc_struct->vcm;
	Vdt* dzName = (Vdt*)trunc_struct->dzName;

	m_truncateDZList.pop_front();
	*/

	/*int ret = -1;
	truncDZ* trunc_struct = (truncDZ *)lpParam;
	TCache* vcm = (TCache*)trunc_struct->vcm;
	Vdt* dzName = (Vdt*)trunc_struct->dzName;
	*/

	/*
	*	Delete Keys from Disk - this part is performed by a separate thread.
	*/
	
	ret = m_dzManager->Rename( *dzName, TC_RENAME_DZ_TRUNCATE );
	if( ret!= 0 )
	{
		m_logStream->print("Error. TCache::Rename. Error renaming DZone partitions \n");
		return ret;
	}

	/*
	*	This part is handled by a separate thread
	*/
	ret = m_diskDB->Truncate( dzName );
	if( ret!= 0 )
	{
		m_logStream->print("Error. TCache::Truncate. Error deleting from list \n");
		//return ret;
	}

	ret = m_dzManager->Rename( *dzName, TC_RENAME_DZ_ORIGINAL );
	if( ret!= 0 )
	{
		m_logStream->print("Error. TCache::Rename. Error renaming DZone partitions \n");
		//return ret;
	}
	/*
	*	Delete Keys from Disk - END
	*/

	//delete trunc_struct;
	return ret;
}

DWORD WINAPI TCache::TruncateDZThread( LPVOID lpParam ) 
{
	TCache *vcm = (TCache*)lpParam;
	DWORD waitTime=INFINITE;
	
	bool timeout=false;
	
	do {
			
		if(vcm->m_shutdownFlag>0)
			waitTime=1000;

		int dwEvent=WaitForSingleObject(vcm->m_semaphoreDatazoneDelete,waitTime);
		
		switch (dwEvent)
		{

			case WAIT_OBJECT_0:
			{
				//int listSize = vcm->m_truncateDZList.size();
				if( vcm->m_truncateDZList.empty() )
					break;

				//truncDZ* trunc_struct;
				//trunc_struct = (truncDZ*)vcm->m_truncateDZList.front();
				Vdt* dzName = (Vdt*)vcm->m_truncateDZList.front();
				vcm->m_truncateDZList.pop_front();
				if( dzName == NULL )
					break;

				int ret = vcm->TruncateDZThread_W(dzName);

				break;
			}

			case WAIT_ABANDONED:
				vcm->m_logStream->print("The event was non-signaled.\n");
				break;

			case WAIT_TIMEOUT:
				//m_logStream->print("The time-out interval elapsed, the event was non-signaled.\n");
				timeout=true;
				break;
		}
		//printf("Return value %d\n",dwEvent);
	}
	while (timeout==false);
	return 0;
}



void TCache::PrintStats( std::ostream& out )
{
	out << "Victimize Stats: \n";
	out << "# Victimize by Records : " << m_num_victimizebyrecords << endl;
	out << "# Victimize by Size : " << m_num_victimizebysize << endl;
}


void TCache::DumpCache( std::ostream& out )
{
	if( m_MemoryEnabled )
	{
		ContextStructure *c_struct;
		m_contextManager->GetFreeContext( c_struct );
		m_qDB->printContents( m_cachingAlgorithm, 0, c_struct, out );
		//m_cacheDB->printContents( 0, c_struct, out );
		m_contextManager->ReleaseContext( c_struct );
	}
}

int TCache::UpdateStats( const stats& stats_update, const int& partition_ID )
{
	if( stats_update.numQ != 0 )
	{
		if( stats_update.numQ > 0 )
		{
			for( int i = 0; i < stats_update.numQ; i++ )				
				m_statsManager->IncrementNumQ( partition_ID );
		}
		else
		{
			for( int i = stats_update.numQ; i < 0; i++ )
				m_statsManager->DecrementNumQ( partition_ID );
		}

		if( m_statsManager->GetAvgQ( partition_ID ) < 0 )
		{
			// TODO: debugging code
			ContextStructure* c_struct;
			m_contextManager->GetFreeContext( c_struct );
			std::ofstream fout;
			fout.open( "QDB_Contents.txt", ios::out );
			//m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, std::cout );
			m_qDB->printContents( m_cachingAlgorithm, partition_ID, c_struct, fout );
			fout.close();
			m_contextManager->ReleaseContext( c_struct );
		}
	}

	if( stats_update.totalQ != 0.0 )
	{
		m_statsManager->IncreaseTotalQ( partition_ID, stats_update.totalQ );
	}

	if( stats_update.L != 0.0 )
	{
		m_statsManager->SetL( partition_ID, stats_update.L );
	}

	if( stats_update.NumRequests != 0 )
	{
		m_statsManager->IncrementNumRequests( partition_ID );
	}

	if( stats_update.NumRequestsKeyFound != 0 )
	{
		m_statsManager->IncrementNumRequestsKeyFound( partition_ID );
	}

	if( stats_update.NumCacheHits != 0 )
	{
		m_statsManager->IncrementNumCacheHits( partition_ID );
	}

	if( stats_update.NumBytesAccessedTotal != 0 )
	{
		m_statsManager->IncreaseNumBytesAccessedTotal( partition_ID, stats_update.NumBytesAccessedTotal );
	}

	if( stats_update.NumBytesCacheHit != 0 )
	{
		m_statsManager->IncreaseNumBytesCacheHit( partition_ID, stats_update.NumBytesCacheHit );
	}

	if( stats_update.NumEvictions != 0 )
	{
		m_statsManager->IncreaseNumEvictions( partition_ID, stats_update.NumEvictions );
	}

	if( stats_update.NumGets != 0 )
	{
		m_statsManager->IncrementNumGets( partition_ID );
	}

	if( stats_update.NumDeletes != 0 )
	{
		m_statsManager->IncrementNumDeletes( partition_ID );
	}

	if( stats_update.NumInserts != 0 )
	{
		m_statsManager->IncrementNumInserts( partition_ID );
	}

	if( stats_update.cleanBytes != 0 )
	{
		m_statsManager->IncreaseCleanBytes( partition_ID, stats_update.cleanBytes );
	}

	if( stats_update.dirtyBytes != 0 )
	{
		m_statsManager->IncreaseDirtyBytes( partition_ID,  stats_update.dirtyBytes );
	}

	if( stats_update.resetFreespace )
	{
		m_statsManager->SetFreeSpaceBytes( partition_ID, stats_update.freespaceBytes );
	}
	else if( stats_update.freespaceBytes != 0 )
	{
		m_statsManager->IncreaseFreeSpaceBytes( partition_ID, stats_update.freespaceBytes );
	}

	if( stats_update.NumVictimizeFailures != 0 )
	{
		m_statsManager->IncreaseNumVicimizeFailures( partition_ID, stats_update.NumVictimizeFailures );
	}

	if( stats_update.NumDeadlocks != 0 )
	{
		m_statsManager->IncreaseNumDeadlocks( partition_ID, stats_update.NumDeadlocks );
	}

	if( stats_update.lastEvictedAgeValue != 0 )
	{
		m_statsManager->SetLastEvictedAgeValue( partition_ID, stats_update.lastEvictedAgeValue );
	}

	
	return 0;
}

int	TCache::Victimize( const int& targetToEvict, const int& evictTypeFlag, const int& partition_ID, stats& stats_update )
{
	int ret = -1;	
	int targetAgg = 0;
	int size;
	Dbc* cursorp = NULL;
	DbTxn* txn = NULL;
	bool cursorClosed = false;
	bool lockAcquired = false;

	if(evictTypeFlag == TC_VICTIMIZE_BY_SIZE)
		InterlockedIncrement( &m_num_victimizebysize );
	else if(evictTypeFlag == TC_VICTIMIZE_BY_RECORD)
		InterlockedIncrement( &m_num_victimizebysize );

	int cache_flags = m_cachingAlgorithm->GetFlags();	

	ContextStructure* c_struct = NULL;
	if( m_contextManager->GetFreeContext( c_struct ) != 0 )
		m_logStream->print( "Error. Couldn't get free context\n" );

	QDBKey qKey( c_struct );
	QDBValue qValue( c_struct );
	CacheKey cKey( c_struct );

	char empty_char = 0;
	Vdt dzNameEmpty( &empty_char, sizeof(empty_char) );
	Vdt keyEmpty( &empty_char, sizeof(empty_char) );
	char* qKeyData;
	int qKeySize;

	int victimCount = 0;
	TCQValue old_q_value;

	Vdt vDzName;
	Vdt vKey;
	
	while( targetAgg < targetToEvict )
	{
		if( m_qDB->GetDBCursor( cursorp, txn, partition_ID ) != 0 )
			m_logStream->print( "Error. Couldn't open cursor on QDB\n" );

		qKey.setValues( TC_CLEAN_FLAG, 0.0, dzNameEmpty, keyEmpty );
		qKey.getData( qKeyData, qKeySize );

		Dbt dbtQKey( qKeyData, qKeySize );
		dbtQKey.set_ulen( c_struct->qdb_key_size );
		dbtQKey.set_flags( DB_DBT_USERMEM );

		Dbt dbtQValue( c_struct->qdb_value, c_struct->qdb_value_size );
		dbtQValue.set_ulen( c_struct->qdb_value_size );
		dbtQValue.set_flags( DB_DBT_USERMEM );
		
		cursorClosed = false;
		victimCount = 0;		

		try
		{
			//position the cursor at the beginning of clean records
			ret = cursorp->get( &dbtQKey, &dbtQValue, DB_SET_RANGE );
		}
		catch( DbException& ex )
		{
			ret = ex.get_errno();
			if( ret == DB_LOCK_DEADLOCK )
			{
				m_logStream->print( "Error. Encountered deadLock while trying to Get DB_SET_RANGE. \n" );
			}
			cursorClosed = true;						
			cursorp->close();
			txn->abort();
		}
		
		if( ret != 0 )
		{
			// Could not retrieve record from QDB, so exit loops
			targetAgg = targetToEvict;
		}

		while( victimCount < TC_NUM_CURSOR_EVICTIONS && ret == 0 )
		{
			try
			{
				qKey.setValues( dbtQKey.get_data(), dbtQKey.get_size() );					
				int debug_int = *(int*)qKey.getKey().get_data();

				if( qKey.getHeaderFlag() != TC_CLEAN_FLAG )
				{						
					victimCount = TC_NUM_CURSOR_EVICTIONS + 1;
					targetAgg = targetToEvict;
					m_logStream->print( "Error. Cache has no more clean record\n" );
				}
				else
				{		
					qValue.setValues( dbtQValue.get_data(), dbtQValue.get_size() );

					size = qValue.getRecordSize();
					vDzName = qKey.getDzName();
					vKey = qKey.getKey();
					old_q_value = qKey.getQValue();

					cKey.setValues( vDzName, vKey );

					if( m_lock->acquireFail( vDzName, vKey ) == 0 )
					{		
						lockAcquired = true;

						if( (cache_flags & CachingAlgorithm::CA_FLAG_MAINTAIN_HISTORY) == 0 )
						{
							// Algorithm does not maintain history, so just delete the
							//  metadata records

							//m_qDB->CursorDelete( cursorp, txn );
							if( cursorp->del( 0 ) != 0 )
								m_logStream->print( "Error. Could not evict qdb record\n" );	
						}


						if( size >= 0 )
						{
							// Record is not a history record therefore has a corresponding
							//  CacheDB data record
							if( m_cacheDB->DeleteDataRec( cKey, partition_ID, &stats_update ) == 0 )
							{								
								//Update structure for Statistics
								stats_update.totalQ -= old_q_value.getFraction();
								stats_update.numQ--;

								stats_update.lastEvictedAgeValue = old_q_value.getInt();

								// Don't update L value if this is an old aged object
								if( old_q_value.getInt() == m_agingMechanism->getAgingValue() )
									stats_update.L = old_q_value.getFraction();

								stats_update.cleanBytes -= qValue.getRecordSize();
								stats_update.NumEvictions++;
								stats_update.freespaceBytes += qValue.getRecordSize();
							}
							else
								m_logStream->print( "Error. Could not evict Data rec\n" );
						}

						if( (cache_flags & CachingAlgorithm::CA_FLAG_MAINTAIN_HISTORY) != 0 )
						{
							// Algorithm maintains history, so we need to keep the records
							//  around in a different form that indicates they are historical
							qValue.setRecordSize( -1 );

							if( cursorp->put( NULL, &dbtQValue, DB_CURRENT ) != 0 )
								m_logStream->print( "Error. Could not update qdb record to history\n" );
						}
						else
						{
							// Algorithm does not maintain history, so just delete the
							//  metadata records
							if( m_cacheDB->DeleteMDLookup( cKey, partition_ID, &stats_update ) != 0 )
								m_logStream->print( "Error. Could not evict md lookup\n" );							
						}

						m_lock->release( vDzName, vKey );
						lockAcquired = false;

						victimCount++;

						if(evictTypeFlag == TC_VICTIMIZE_BY_SIZE)
							targetAgg += size;
						else if(evictTypeFlag == TC_VICTIMIZE_BY_RECORD)
							targetAgg++;

						if( targetAgg > targetToEvict )
							victimCount = TC_NUM_CURSOR_EVICTIONS + 1;

					}
					
					/* Move cursor to the next record on the index
					 * If no more records found, exit the outer loop
					 //*/
					ret = cursorp->get( &dbtQKey, &dbtQValue, DB_NEXT );
					if( ret != 0 )
						targetAgg = targetToEvict;
				}
			}
			catch( DbException& e )
			{
				std::string err_msg = e.what();
				err_msg += " Victim #";
				err_msg += TCUtility::intToString( victimCount );
				err_msg += "\n";
				m_logStream->print( err_msg );				

				// Resizes the value buffer if it is too small
				if( e.get_errno() == DB_BUFFER_SMALL )
				{
					if( dbtQValue.get_size() > dbtQValue.get_ulen() )
					{
						// Resize the value
						ContextManager::ResizeMemory( c_struct->qdb_value, dbtQValue.get_size() );
						c_struct->qdb_value_size = dbtQValue.get_size();

						dbtQValue.set_data( c_struct->qdb_value );
						dbtQValue.set_ulen( c_struct->qdb_value_size );
					}
					else if( dbtQKey.get_size() > dbtQKey.get_ulen() )
					{
						// Resize the key
						ContextManager::ResizeMemory( c_struct->qdb_key, dbtQKey.get_size() );
						c_struct->qdb_key_size = dbtQKey.get_size();
					}
					else
					{
						m_logStream->print( "VictimizeSize - Error. Neither the key nor value sizes are too small.\n" );
					}
				}
				else
				{
					// On a deadlock for cursor deletes, close cursor and abort transaction
					std::string debug_key = TCUtility::intToString( *(int*)qKey.getKey().get_data() );

					stats_update.NumDeadlocks++;

					if( !cursorClosed )
					{
						cursorClosed = true;						
						cursorp->close();
						txn->abort();
						
						m_logStream->print( "Error. Encountered deadlock while trying to delete " + debug_key + " from cursor\n" );
					}
					else
						m_logStream->print( "Error. Cursor was closed unexpectily " + debug_key + "\n" );
				}

				if( lockAcquired )
				{
					m_lock->release( vDzName, vKey );
					lockAcquired = false;
				}

				//Always exit inner loop in case of an exeption. -> 
				//Try position again at the top of the tree to see if there are any records left
				victimCount = TC_NUM_CURSOR_EVICTIONS + 1;
			}

		}
		if( !cursorClosed )
		{
			cursorClosed = true;
			cursorp->close();
			txn->commit( 0 );
		}
	}

	m_contextManager->ReleaseContext( c_struct );

	return ret;
}



int	TCache::EvictAndInsertDataRec( CacheKey& cKey, CacheValue& cValue, const int& partition_ID, stats& stats_update )
{
	int numTries = 0;
	int ret = -1;

	while( ret != 0 && numTries < m_MaxVictimSelectionAttempts )
	{
		numTries++;
		ret = m_cacheDB->InsertDataRec( cKey, cValue, partition_ID, &stats_update );
		if( ret == TC_FAILED_MEM_FULL )
		{
			stats_update.freespaceBytes = 0;
			stats_update.resetFreespace = true;

			if( Victimize( cValue.getDataRecSize() * m_MaxVictimSelectionSizeMultiple, TC_VICTIMIZE_BY_SIZE ,partition_ID, stats_update ) != 0 )			
				m_logStream->print( "Error. Could not victimize by size\n" );
		}
		else if( ret != 0 )
		{
			m_logStream->print( "Error. Encountered error trying to evict and insert that wasn't due to memory being full\n" );
		}
	}

	if( numTries == m_MaxVictimSelectionAttempts && ret != 0 )
		stats_update.NumVictimizeFailures++;


	return ret;
}

int	TCache::EvictAndInsertMDLookup( CacheKey& cKey, CacheValue& cValue, const int& partition_ID, stats& stats_update )
{
	int numTries = 0;
	int ret = -1;

	while( ret != 0 && numTries < m_MaxVictimSelectionAttempts )
	{
		numTries++;
		ret = m_cacheDB->InsertMDLookup( cKey, cValue, partition_ID, &stats_update );
		if( ret == TC_FAILED_MEM_FULL )
		{
			stats_update.freespaceBytes = 0;
			stats_update.resetFreespace = true;

			if( Victimize( m_MaxVictimSelectionByRecordsMultiple, TC_VICTIMIZE_BY_RECORD, partition_ID, stats_update ) != 0 )			
				m_logStream->print( "Error. Could not victimize by records\n" );
		}
		else if( ret != 0 )
		{
			m_logStream->print( "Error. Encountered error trying to evict and insert that wasn't due to memory being full\n" );
		}
	}

	if( numTries == m_MaxVictimSelectionAttempts && ret != 0 )
		stats_update.NumVictimizeFailures++;

	return ret;
}

int	TCache::EvictAndInsertQDBRec( QDBKey& qKey, QDBValue& qValue, const int& partition_ID, stats& stats_update )
{
	int numTries = 0;
	int ret = -1;

	while( ret != 0 && numTries < m_MaxVictimSelectionAttempts )
	{
		numTries++;

		ret = m_qDB->Insert( qKey, qValue, partition_ID, &stats_update );
		if( ret == TC_FAILED_MEM_FULL )
		{
			stats_update.freespaceBytes = 0;
			stats_update.resetFreespace = true;

			if( Victimize( m_MaxVictimSelectionByRecordsMultiple, TC_VICTIMIZE_BY_RECORD, partition_ID, stats_update ) != 0 )
				m_logStream->print( "Error. Could not victimize by records\n" );
		}
		else if( ret != 0 )
		{
			m_logStream->print( "Error. Encountered error trying to evict and insert that wasn't due to memory being full\n" );
		}
	}

	if( numTries == m_MaxVictimSelectionAttempts && ret != 0 )
	{
		stats_update.NumVictimizeFailures++;
		std::string msg = "Error. Could not insert QDBRec after evicting ";
		msg += TCUtility::intToString( numTries );
		msg += " times.\n";
		m_logStream->print( msg ); 

	}

	/* TODO: test code. will compact help with memory utilization?
	if( ret != 0 && numTries == TC_MAX_RETRIES )
	{
		Db* dbp;
		m_qDB->getDbp( partition_ID, dbp );
		if( dbp->compact( NULL, NULL, NULL, NULL, DB_FREE_SPACE, NULL ) != 0 )
			m_logStream->print( "Error. Could not compact QDB\n" );
	}
	//*/

	return ret;
}

int TCache::GetLowestDirtyQObjectFromAllQDBS(QDBKey &min_key,QDBValue &min_value,int  &min_partitionID) 
{
	int ret=0;
	ContextStructure *cs;
	
	try {
		
		m_contextManager->GetFreeContext(cs);
		
		QDBKey key(cs);
		QDBValue value(cs);
		min_partitionID=-1;
		
		// Set the minimum Q to the first object found in the QDB
		bool minQSetToMinimum=false;
		TCQValue minQ(0,-1.0);
		
		for(int i=0;i<m_numPartitions;i++)
		{
			if(m_statsManager->GetDirtyBytes(i)!=0)
			{
				ret=m_qDB->GetLowestDirtyRecord(key,value,i);
				if(ret!=0)
				{
					//printf("Get lowest bytes returned -1");
					continue;
				}
				if(key.getQValue()<minQ || !minQSetToMinimum)
				{
					if(!minQSetToMinimum)
						minQSetToMinimum=true;

					minQ=key.getQValue();
					
					min_partitionID=i;
				
					// copy to min key and value
					char *data=NULL;
					int size=0;
					
					key.getData(data,size);
			
					min_key.setValues((void *)data,size,true);
					
					value.getData(data,size);
					min_value.setValues((void *)data,size);
					min_value.getData(data,size);
					min_value.setValues((void *)data,size);
					
					//printf("iter %d Key is %d and Q %e partition id %d\n",i,*(int *)(min_key.getKey().get_data()),min_key.getQValue(),min_partitionID);
				}

			}
		}
		m_contextManager->ReleaseContext(cs);	
		
		// If there are no dirty records
		if(min_partitionID==-1)
		{
			//m_logStream->print("Error no dirty records found\n");
			//printf("Error no dirty records found\n");
			return -1;
		}
		else
			ret=0;
	}	
	catch(DbException &e) {
		m_logStream->print(e.what());
		m_contextManager->ReleaseContext(cs);	
		return (e.get_errno());
	} catch(exception &e) {
		m_logStream->print(e.what());
		m_contextManager->ReleaseContext(cs);
		return (-1);
	}   
	return ret;
}



// Read the minQ
// Modify the cache
// Write to disk
int TCache::BackgroundThread_RMW()
{
	int partitionID=0;
	ContextStructure *cs;
	m_contextManager->GetFreeContext(cs);
	
	QDBKey qdb_key(cs);
	QDBValue qdb_value(cs);
	
	CacheKey cacheKey(cs);
	CacheValue cacheValue(cs);

	Vdt vdt_value;
	
	
	bool iCacheDBK1=false, iCacheDBK2=false, iQDB=false;

	bool insert_failure=false;

	bool dontUpdateStats=false;

	int key_val=0; 

	long long dirtyBytes=0;
	long long cleanBytes=0;

	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );

	int ret=GetLowestDirtyQObjectFromAllQDBS(qdb_key,qdb_value, partitionID); 
	if(ret==0)
	{
		Vdt dzname=qdb_key.getDzName();
		Vdt key=qdb_key.getKey();
		key_val = *(int*)qdb_key.getKey().get_data();
		int size=qdb_value.getRecordSize();
		TCQValue Q=qdb_key.getQValue();
		double L=m_statsManager->GetL(partitionID);
		int lastEvictedAgeValue = m_statsManager->GetLastEvictedAgeValue(partitionID);
		dirtyBytes=size;

		int diskPartitionID= TCUtility::bernsteinHash(dzname,key,m_numDiskPartitions);
		//std::string debug_dz = "";
		//debug_dz.insert( 0, dzname.get_data(), dzname.get_size() );
		//m_logStream->debug_print("Background Thread inserting Key "+ITS(key_val)+" dzname "+ debug_dz +" partition id "+ITS(diskPartitionID)+"\n");

		
		cacheKey.setValues(dzname,key);
		
		m_lock->acquireWait(dzname,key); 
	
		//Check if the cache db record is not conssistent with the qdb record delete it if not
		// possible interation between get and insert );
		ret=m_cacheDB->GetMDLookup(cacheKey,cacheValue,partitionID);
		if(ret==0) {
			if(cacheValue.getDirtyBit() == TC_CLEAN_FLAG)
			{
				m_logStream->print("Background Thread QDB entry not consistent with cache DB entry...cache K2 is clean\n");
				if(m_qDB->Delete(qdb_key,partitionID)!=0)
					m_logStream->print( "TC::Background Thread- Could not delete old record from QDB\n" );
				m_contextManager->ReleaseContext(cs);
				m_lock->release(dzname,key);
				return -1;
			}
			else if (cacheValue.getQValue() != Q)
			{
				m_logStream->print("Background Thread QDB entry not consistent with cache DB entry...Q dont match\n");
				if(m_qDB->Delete(qdb_key,partitionID)!=0)
					m_logStream->print( "TC::Background Thread- Could not delete old record from QDB\n" );
				m_contextManager->ReleaseContext(cs);
				m_lock->release(dzname,key);	
				return -1;
			}
		}


		// Check if the background thread buffer is big enough
		if(size > m_backgroundThreadBufferSize)
		{
			delete[] m_backgroundThreadBuffer;
			m_backgroundThreadBufferSize=size;
			m_backgroundThreadBuffer= new char[size];
			m_logStream->print("Background Thread:: Resized buffer to "+ITS(size)+"\n");
		}
		
		Dbt dbt_value(m_backgroundThreadBuffer,m_backgroundThreadBufferSize);
		dbt_value.set_ulen(m_backgroundThreadBufferSize);
		dbt_value.set_flags( DB_DBT_USERMEM );
	
		//Look up record from cache
		ret=m_cacheDB->GetDataRec(cacheKey,&dbt_value,partitionID);
		if(ret==DB_BUFFER_SMALL)
		{
			int req_size=dbt_value.get_size();
			if(req_size > m_backgroundThreadBufferSize)
			{
				size=req_size;
				delete[] m_backgroundThreadBuffer;
				m_backgroundThreadBufferSize=size;
				m_backgroundThreadBuffer= new char[size];
				m_logStream->print("Background Thread:: Resized buffer to "+ITS(size)+"\n");
				dbt_value.set_size(m_backgroundThreadBufferSize);
				dbt_value.set_ulen(m_backgroundThreadBufferSize);
				ret=m_cacheDB->GetDataRec(cacheKey,&dbt_value,partitionID);
			}
			else {
				m_logStream->print("Background Thread:: DB_BUFFER_SMALL error \n");
			}
		}
					
		if(ret!=0)
		{
			m_logStream->print("Background Thread::Error Could not get data record from  cachedb "+TCUtility::intToString(ret)+"\n");
			
			iQDB=true,iCacheDBK1=true,iCacheDBK2=true;
			insert_failure=true;
			if (ret==DB_NOTFOUND) 
			{
				dontUpdateStats=true;
				insert_failure=false;
			}
			goto failure;
		}

		//Insert the record to disk
		vdt_value.set_data(m_backgroundThreadBuffer);
		vdt_value.set_size(dbt_value.get_size());
		ret=m_diskDB->Put(dzname,key,vdt_value,&stats_update);
		if(ret!=0)
		{
			m_logStream->print("Background Thread::Error Could not write record to disk "+TCUtility::intToString(ret)+"\n");
			//printf("Background Thread::Error Could not write record to disk \n",ret);
			iQDB=true,iCacheDBK1=true,iCacheDBK2=true;
			insert_failure=true;
			goto failure;
		}

		ret=m_qDB->Delete(qdb_key,partitionID);
		if(ret!=0)
		{
			// No error check is required here ..it would insert a clean qdb record and once it checks if cache db entry is consistent and delete it
			printf("Background Thread::Error Could not delete from qdb key=%d Q=%e ret=%d partitionid %d\n",key_val,qdb_key.getQValue(), ret,partitionID);
			
			// Increment the semaphore because we are not able to delete the qdb entry
			if (!ReleaseSemaphore(m_semaphore,  // handle to semaphore
							1,                      // increase count by one
							NULL))            // not interested in previous count
			{
				m_logStream->print("Background Thread::failed to signal semaphore\n"); 
				return -1;
			}
		}
		else {
			//dirtyBytes=size;
			InterlockedIncrement(&m_num_clean_to_dirty);
		}
		
		qdb_key.setHeaderFlag(TC_CLEAN_FLAG);
		ret=m_qDB->Insert(qdb_key,qdb_value,partitionID);
		
		if(ret!=0)
		{
			if(ret==TC_FAILED_MEM_FULL)
				ret=EvictAndInsertQDBRec(qdb_key,qdb_value,partitionID,stats_update);
		}
		if(ret!=0) {
			m_logStream->print("Background Thread::Error Could not insert clean record to qdb \n"+TCUtility::intToString(ret));
			iCacheDBK1=true, iCacheDBK2=true;
			goto failure;
		}
		else {
			cleanBytes+=size;
		}
	
		//Insert clean object to cache
		cacheValue.setMDValues(TC_CLEAN_FLAG,Q);
		ret=m_cacheDB->InsertMDLookup(cacheKey,cacheValue,partitionID);
		
		if(ret!=0)
		{
			if(ret==TC_FAILED_MEM_FULL)
			{
				ret=EvictAndInsertMDLookup(cacheKey,cacheValue,partitionID,stats_update);
			}
		}

		if(ret!=0) {
			m_logStream->print("Background Thread::Error Could not insert clean record to cachedb \n"+TCUtility::intToString(ret));
			iQDB=true,iCacheDBK1=true,iCacheDBK2=true;
			goto failure;
		}
		


		m_lock->release(dzname,key);

		//m_statsManager->DecreaseDirtyBytes(partitionID,dirtyBytes);
		
		dirtyBytes=-dirtyBytes;
		m_statsManager->IncreaseDirtyBytes(partitionID,dirtyBytes);
		m_statsManager->IncreaseCleanBytes(partitionID,cleanBytes);

		//Update stats from victim selections
		UpdateStats(stats_update,partitionID);

		
		m_contextManager->ReleaseContext(cs);
		
		return ret;

failure:

		if(iCacheDBK1)
		{

			if(m_cacheDB->DeleteDataRec(cacheKey,partitionID)!=0)
				m_logStream->print( "TC::Background Thread- Could not delete old Data Record from CacheDB\n" );
		}
		if(iCacheDBK2)
		{
			if(m_cacheDB->DeleteMDLookup(cacheKey,partitionID)!=0)
				m_logStream->print( "TC::Background Thread- Could not delete old MD lookup from CacheDB\n" );
		}
		if(iQDB)
		{
			if(m_qDB->Delete(qdb_key,partitionID)!=0)
				m_logStream->print( "TC::Background Thread- Could not delete old record from QDB\n" );
		}
		if(insert_failure)
			printf("Background Thread could not insert key %d \n",key_val);
		

		m_contextManager->ReleaseContext(cs);
		m_lock->release(dzname,key);
		if(!dontUpdateStats)
		{
			dirtyBytes=-dirtyBytes;
			m_statsManager->IncreaseDirtyBytes(partitionID,dirtyBytes);
			stats_update.numQ--;
			stats_update.totalQ-=qdb_key.getQValue().getFraction();
			UpdateStats(stats_update,partitionID);
		}
		
		return ret;
	}
	else {
		//printf("BackgroundThread::No dirty object\n");
		//m_logStream->print("BackgroundThread::No dirty object\n");
	}

	m_contextManager->ReleaseContext(cs);
	return ret;
}


DWORD WINAPI TCache::BackgroundThread( LPVOID lpParam ) 
{
	LARGE_INTEGER end;
	TCache *vcm = (TCache *)lpParam;
	DWORD waitTime=INFINITE;
	
	bool timeout=false;
	
	vcm->m_backgroundThreadBuffer=new char [vcm->m_backgroundThreadBufferSize];
		
	do {
			
		if(vcm->m_shutdownFlag>0)
			waitTime=1000;
		
		int dwEvent=WaitForSingleObject(vcm->m_semaphore,waitTime);
		
		switch (dwEvent)
		{

			case WAIT_OBJECT_0:
				// Read the minQ
				// Modify the cache
				// Write to disk
				vcm->BackgroundThread_RMW();

				InterlockedIncrement(&vcm->m_num_signal);
				break;

			case WAIT_ABANDONED:
				vcm->m_logStream->print("The event was non-signaled.\n");
				break;

			case WAIT_TIMEOUT:
				//m_logStream->print("The time-out interval elapsed, the event was non-signaled.\n");
				timeout=true;
				break;
		}
	}
	while (timeout==false);

	QueryPerformanceCounter(&end);

	//printf("Time to run Background Thread %f seconds\n",(double)(end.QuadPart-TCache::m_start.QuadPart)/TCache::m_num_ticks.QuadPart);
	delete [] vcm->m_backgroundThreadBuffer;
	return 0;
}

int TCache::AsynchronousInsert (const Vdt& dzname, const Vdt& Key, const Vdt& Value)
{
	bool iCacheDBK1=false, iCacheDBK2=false, iQDB=false;
	bool iOldQDB=false;

	ContextStructure* cs;
	m_contextManager->GetFreeContext( cs );

	int partitionID = TCUtility::bernsteinHash(dzname, Key, m_numPartitions);
	double L = m_statsManager->GetL( partitionID );
	int lastEvictedAgeValue = m_statsManager->GetLastEvictedAgeValue(partitionID);
	
	
	long long stats_dirtyBytes=0,stats_cleanBytes=0,totalBytes=0;
	stats_dirtyBytes=m_statsManager->GetDirtyBytes(partitionID)+Value.get_size();
	stats_cleanBytes=m_statsManager->GetCleanBytes(partitionID);
	
	if (L==0)
		totalBytes=m_cacheSize/m_numPartitions;
	else {
		totalBytes=stats_dirtyBytes+stats_cleanBytes;
	}

	double dirty_clean_ratio= (double)(stats_dirtyBytes)/totalBytes;
	if( dirty_clean_ratio > m_asynchronousToSynchronousRatio && m_statsManager->GetPartitionMode(partitionID) )
	{
		//m_logStream->print("TC::AsyncInsert Switched to synchronous Insert Partition id " +ITS(partitionID)+"\n");
		m_statsManager->SetPartitionMode(partitionID,false);
		m_contextManager->ReleaseContext(cs);
		return TC_DIRTY_CLEAN_RATIO_FAILURE;
	}
		
	int cacheFlags = m_cachingAlgorithm->GetFlags();
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter( &currTimestamp );	

	double avgQ = m_statsManager->GetAvgQ( partitionID );
	TCQValue newQ;
	
	long long cleanBytes=0;
	long long dirtyBytes=0;	
	long long oldDirtyBytes=0;

	// Initialize Stats update struct to keep track of changes to stats.
	// This is used to delay the update calls to StatsManager to outside the Lock blocks
	//  since StatsManager uses acquireWait internally.
	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );

	int ret = 0;
	
	
	bool exists=false; // This flag signifies whether the metadata exists in the cache or not
	char D=TC_DIRTY_FLAG;
	TCQValue oldQ(0,-1.0);
	void *metadata=NULL;

	CacheKey cacheKeyK2(cs);
	CacheValue cacheValueK2(cs);
	cacheKeyK2.setValues(dzname, Key);
	
	CacheKey cacheKeyK1 (cs);
	CacheValue cacheValueK1(cs);
	cacheKeyK1.setValues(dzname,Key);
	
	cacheValueK1.setDataRecValues(Value);

	QDBKey qKey(cs);
	QDBValue qValue(cs);

	m_lock->acquireWait(dzname,Key);

	ret= m_cacheDB->GetMDLookup(cacheKeyK2,cacheValueK2,partitionID);
	if (ret==DB_LOCK_DEADLOCK)
		goto failure;
	
	if(ret==0){
		exists=true;
		D=cacheValueK2.getDirtyBit();
		oldQ=cacheValueK2.getQValue();
	}
	else
		exists=false;

	if(exists)
	{
		qKey.setValues(D, oldQ, dzname,Key);
		ret=m_qDB->Get(qKey, qValue,partitionID);
		if(ret!=0)
		{
			m_logStream->print( "TC:AsyncInsert Could not find key in QDB\n" );
			iCacheDBK1=true,iCacheDBK2=true;
			goto failure;
		}
		iCacheDBK1=true,iCacheDBK2=true;
		iOldQDB=true;

		if(D==TC_DIRTY_FLAG)
		{
			if(qValue.getRecordSize()>0)
				oldDirtyBytes-=qValue.getRecordSize();
		}
		else {
			if(qValue.getRecordSize()>0)
				cleanBytes-=qValue.getRecordSize();
		}

		metadata = qValue.getMetadataPtr();
		m_cachingAlgorithm->UpdateRead( metadata );
	}
	else {
		// Race condition between insert and get and if the cache key k2 does not exist 
		if( m_cacheDB->Exists(cacheKeyK1,partitionID))
		{
			m_logStream->print("TC::AsyncInsert There exists a CacheDB K1 record with no cacheDB K2 record\n");
			ret= m_cacheDB->InsertDataRec(cacheKeyK1,cacheValueK1,partitionID);
			if(ret!=0)
			{
				if(ret==TC_FAILED_MEM_FULL)
				{
					ret=EvictAndInsertDataRec(cacheKeyK1,cacheValueK1,partitionID,stats_update);
					if(ret!=0)
						m_logStream->print("TC::AsyncInsert failed to insert Data Rec record even after victimizing\n");
				}
				if(ret!=0)
					iCacheDBK1=true;
				goto failure;
			}
		}
				

		int metadata_size = m_cachingAlgorithm->GetDataSize();
		qKey.setValues(D,oldQ,dzname,Key);
		qValue.setRecordSize(Value.get_size());
		if( qValue.checkMetadataSize( metadata_size ) != 0 )
			m_logStream->print( "TC::AsyncInsert. Could not allocate memory for metadata buffer\n" );

		metadata = qValue.getMetadataPtr();
		m_cachingAlgorithm->GenerateMetadata( dzname, Key, &Value, metadata, metadata_size );
	}

	newQ.setInt( m_agingMechanism->getAgingValue() );
	newQ.setFraction( m_cachingAlgorithm->calcQValue( metadata, L) );
		
	/*if( (cacheFlags & CachingAlgorithm::CA_FLAG_ADMISSION_CONTROL) != 0 )
	{
		if( m_cachingAlgorithm->AdmitKey( newQ, L) == false )
		{
			ret=TC_ADMISSION_CONTROL_FAILURE;
			m_logStream->print("TC::AsyncInsert Key "+ITS(*(int *)Key.get_data())+ " does not pass the admission control criteria\n");
			
			if(exists)
			{
				iCacheDBK1=true;
				iCacheDBK2=true;
				iQDB=true;
			}
			goto failure;
		}
	}*/

	
	ret= m_cacheDB->InsertDataRec(cacheKeyK1,cacheValueK1,partitionID);
	if(ret!=0)
	{
		if(ret==TC_FAILED_MEM_FULL)
		{
			if(EvictAndInsertDataRec(cacheKeyK1,cacheValueK1,partitionID,stats_update)!=0)
			{
				m_logStream->print("TC::AsyncInsert failed to insert Data Rec record even after victimizing\n");
				goto failure;
			}

		}
		else
			goto failure;
	}

	iCacheDBK1=true;
	
	//bool avgQUpdate=true, sameQUpdate=true;
	bool updateMetadata = true;

	if( newQ.getInt() == oldQ.getInt() )
	{
		LARGE_INTEGER currentTimestamp, oldTimestamp;
		currentTimestamp.QuadPart = 0;
		oldTimestamp.QuadPart = 0;

		if( m_cachingAlgorithm->NoNeedToUpdateMetadata( oldQ.getFraction(), newQ.getFraction(), avgQ, 
			oldTimestamp, currentTimestamp, m_CONST_updateDelta ) == true )
		{
			updateMetadata = false;
		}

		/*
		// Don't update metadata if the q value is the same 
		if( (cacheFlags & CachingAlgorithm::CA_FLAG_SAME_Q_NO_UPDATE) != 0 )
		{
			if(newQ == oldQ)
			{
				sameQUpdate=false;
			}
		}

		// Don't update if the q value is already popular ( > avg )
		if( (cacheFlags & CachingAlgorithm::CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE) != 0 )
		{
			if(newQ.getFraction() > avgQ)
			{
				avgQUpdate=false;
			}
		}
		//*/
	}
		
	//if (  (!exists) || (exists && (  (D==TC_CLEAN_FLAG) || (D==TC_DIRTY_FLAG && sameQUpdate && avgQUpdate) ) ) )
	if (  (!exists) || (exists && (  (D==TC_CLEAN_FLAG) || (D==TC_DIRTY_FLAG && updateMetadata) ) ) )
	{
		cacheValueK2.setMDValues(TC_DIRTY_FLAG ,newQ);
		ret= m_cacheDB->InsertMDLookup(cacheKeyK2,cacheValueK2,partitionID);
		if(ret!=0)
		{
			if(ret==TC_FAILED_MEM_FULL)
			{
				if(EvictAndInsertMDLookup(cacheKeyK2,cacheValueK2,partitionID,stats_update)!=0)
				{
					m_logStream->print("TC::AsyncInsert failed to insert CACHE K2 record even after victimizing\n");
					goto failure;
				}
			}
			else
				goto failure;
		}

		iCacheDBK2=true;

		qKey.setQValue(newQ);
		qKey.setHeaderFlag(TC_DIRTY_FLAG);
		qValue.setValues(Value.get_size(),currTimestamp,m_cachingAlgorithm->GetDataSize());
		ret=m_qDB->Insert(qKey,qValue,partitionID);
		if(ret!=0)
		{
			if(ret==TC_FAILED_MEM_FULL)
			{
				if(EvictAndInsertQDBRec(qKey,qValue,partitionID,stats_update)!=0)
				{
					m_logStream->print("TC::AsyncInsert failed to insert QDB record even after victimizing\n");
					goto failure;
				}
			}
			else
				goto failure;
		}
		iQDB=true;
		dirtyBytes+=Value.get_size()+oldDirtyBytes;
		
		if(exists)
		{
			qKey.setHeaderFlag(D);
			qKey.setQValue(oldQ);
			// If the old QDB record is exactly the same, dont delete it
			if(D!=TC_DIRTY_FLAG || oldQ!=newQ) 
			{
				ret=m_qDB->Delete(qKey,partitionID);
				if(ret!=0)
					m_logStream->print("TC::AsyncInsert Could not find oldq to delete\n");
			}
			iOldQDB=false;
			stats_update.totalQ += (newQ - oldQ).getFraction();
		}
		else {
			stats_update.totalQ += newQ.getFraction();
			stats_update.numQ++;
		}
	}

	m_contextManager->ReleaseContext(cs);
	m_lock->release(dzname,Key);

	UpdateStats(stats_update,partitionID);
	
	m_statsManager->IncreaseDirtyBytes(partitionID,dirtyBytes);
	m_statsManager->IncreaseCleanBytes(partitionID,cleanBytes);
	
	//Signal the semaphore
	if (!ReleaseSemaphore(m_semaphore,  // handle to semaphore
							1,                      // increase count by one
							NULL))            // not interested in previous count
	{
		m_logStream->print("TC::AsyncInsert failed to signal semaphore\n"); 
		return -1;
	}
	
	return 0;	


failure:
	if(iCacheDBK1)
	{

		if(m_cacheDB->DeleteDataRec(cacheKeyK1,partitionID)!=0)
			m_logStream->print( "TC::AsyncInsert- Could not delete old Data Record from CacheDB\n" );
	}
	if(iCacheDBK2)
	{
		if(m_cacheDB->DeleteMDLookup(cacheKeyK2,partitionID)!=0)
			m_logStream->print( "TC::AsyncInsert- Could not delete old MD lookup from CacheDB\n" );
	}
	if(iQDB)
	{
		if(m_qDB->Delete(qKey,partitionID)!=0)
			m_logStream->print( "TC::AsyncInsert- Could not delete record from QDB\n" );
	}
	if(iOldQDB)
	{
		qKey.setQValue(oldQ);
		qKey.setHeaderFlag(D);
		if(m_qDB->Delete(qKey,partitionID)!=0)
			m_logStream->print( "TC::AsyncInsert- Could not delete old record from QDB\n" );
	}

	m_logStream->print("TC:: AsyncInsert failed \n");

	m_contextManager->ReleaseContext(cs);
	m_lock->release(dzname,Key);
	
	if(exists)
	{
		stats_update.totalQ-=qKey.getQValue().getFraction();
		stats_update.numQ-=1;
		UpdateStats(stats_update,partitionID);
		m_statsManager->IncreaseDirtyBytes(partitionID,oldDirtyBytes);
		m_statsManager->IncreaseCleanBytes(partitionID,cleanBytes);
	}
	else
		UpdateStats(stats_update,partitionID);
		
	//TODO add to queue 

	return ret;
}
	

int TCache::SynchronousInsert (const Vdt& dzname, const Vdt& Key, const Vdt& Value, int cachePartitionID, bool DoAdmissionControl, bool diskInsert, const double& cost)
{
	int ret=0;
	m_statsManager->ModifyNumSynchronousInsert (1);

	//int cachePartitionID= TCUtility::bernsteinHash(dzname,Key,m_numPartitions);
	//int diskPartitionID= TCUtility::bernsteinHash(dzname,Key,m_numDiskPartitions);
	//m_logStream->debug_print("TC::SyncInsert Inserting Key "+ITS(*(int*)Key.get_data())+" dzname "+dzname.get_data()+" partition id "+ ITS(diskPartitionID)+"\n");

	bool DataRecExistsInCache=false,qdbRecExists=false;

	double L = m_statsManager->GetL( cachePartitionID );
	int lastEvictedAgeValue = m_statsManager->GetLastEvictedAgeValue( cachePartitionID );

	stats stats_update;
	memset( &stats_update, 0, sizeof(stats_update) );

	int cacheFlags = m_cachingAlgorithm->GetFlags();
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter( &currTimestamp );	

	bool iCacheDBK1=false, iCacheDBK2=false, iQDB=false;
	bool admitKey = true;
	bool cacheFailure=false;
	m_lock->acquireWait(dzname,Key);

	ContextStructure* cs;
	m_contextManager->GetFreeContext( cs );
	
	CacheKey cacheKeyK2(cs);
	CacheValue cacheValueK2(cs);
	cacheKeyK2.setValues(dzname, Key);
	
	CacheKey cacheKeyK1 (cs);
	CacheValue cacheValueK1(cs);
	cacheKeyK1.setValues(dzname,Key);
	cacheValueK1.setDataRecValues(Value);

	char oldDirtyByte=TC_CLEAN_FLAG;
	TCQValue oldQ(0,-1.0);

	TCQValue newQ(0,-2.0);
	int partitionID=cachePartitionID;
	QDBKey qKey(cs);
	QDBValue qValue(cs);
	qKey.setValues(oldDirtyByte,oldQ,dzname,Key);
	
	int size=0;

	void *metadata;

	
	DataRecExistsInCache=m_cacheDB->Exists(cacheKeyK1,cachePartitionID);
	if(DataRecExistsInCache)
	{
		iCacheDBK1=true;
		ret=m_cacheDB->GetMDLookup(cacheKeyK2,cacheValueK2,cachePartitionID);
		if(ret==0)
		{
			iCacheDBK2=true;
			oldQ=cacheValueK2.getQValue();
			oldDirtyByte=cacheValueK2.getDirtyBit();
			qKey.setHeaderFlag(oldDirtyByte);
			qKey.setQValue(oldQ);
			
			if(m_qDB->Get(qKey, qValue,partitionID)!=0) {
				//printf("TC::SyncInsert could not find QDB key %d datazone %s",(*(int*)Key.get_data()),dzname.get_data());
				m_logStream->print( "TC:SyncInsert Could not find key in QDB\n" );
				qdbRecExists=false;
			}
			else {
				qdbRecExists=true;
				size=qValue.getRecordSize();
				metadata = qValue.getMetadataPtr();
				m_cachingAlgorithm->UpdateRead( metadata );
			}
		}
		else {
			m_logStream->print( "TC:SyncInsert Could not find key K2 in CacheDB \n" );
		}
	}

	if(!DataRecExistsInCache | !qdbRecExists)
	{
		int metadata_size = m_cachingAlgorithm->GetDataSize();
		if( qValue.checkMetadataSize( metadata_size ) != 0 )
			m_logStream->print( "TC::SyncInsert. Could not allocate memory for metadata buffer\n" );

		metadata = qValue.getMetadataPtr();
		CachingAlgorithmInputData algorithm_data;
		algorithm_data.dzName = &dzname;
		algorithm_data.Key = &Key;
		algorithm_data.value = &Value;
		algorithm_data.cost = cost;

		// TODO: Currently only supports cost for a memory-only system
		if( m_MemoryEnabled && !m_DiskEnabled )
			m_cachingAlgorithm->GenerateMetadata( algorithm_data, metadata, metadata_size );
		else
			m_cachingAlgorithm->GenerateMetadata( dzname, Key, &Value, metadata, metadata_size );

		qValue.setRecordSize(Value.get_size());
	}	

	if(DoAdmissionControl)
	{
		newQ.setFraction( m_cachingAlgorithm->calcQValue( metadata, L) );
		newQ.setInt( m_agingMechanism->getAgingValue() );
		
		if( (cacheFlags & CachingAlgorithm::CA_FLAG_ADMISSION_CONTROL) != 0 )
		{
			// This check considers that there are very aged objects in the cache
			//  and new objects should be admitted to kick them out
			if( lastEvictedAgeValue < m_agingMechanism->getAgingValue() - 2 )
			{
				admitKey = true;
			}
			else
			{
				// Checks the Algorithm-specific admission control, based on the
				//  q value and l value
				admitKey=m_cachingAlgorithm->AdmitKey( newQ.getFraction(), L);
				if (admitKey==false)
					ret=TC_ADMISSION_CONTROL_FAILURE;
			}
		}

		if(admitKey)
		{
			ret= m_cacheDB->InsertDataRec(cacheKeyK1,cacheValueK1,partitionID);
			if(ret!=0)
			{
				if(ret==TC_FAILED_MEM_FULL)
				{
					if(EvictAndInsertDataRec(cacheKeyK1,cacheValueK1,partitionID,stats_update)!=0)
					{
						//m_logStream->print("TC::SyncInsert failed to insert data record even after victimizing\n");
						cacheFailure=true;
						goto failure;
					}

				}
				else {
					cacheFailure=true;
					goto failure;
				}
			}
			iCacheDBK1=true;
			
			cacheValueK2.setMDValues(TC_CLEAN_FLAG ,newQ);
			ret= m_cacheDB->InsertMDLookup(cacheKeyK2,cacheValueK2,partitionID);
			if(ret!=0)
			{
				if(ret==TC_FAILED_MEM_FULL)
				{
					if(EvictAndInsertMDLookup(cacheKeyK2,cacheValueK2,partitionID,stats_update)!=0)
					{
						//m_logStream->print("TC::SyncInsert failed to insert cacheK2 record even after victimizing\n");
						cacheFailure=true;
						goto failure;
					}
				}
				else {
					cacheFailure=true;
					goto failure;
				}
			}
			
			iCacheDBK2=true;

			qKey.setQValue(newQ);
			qKey.setHeaderFlag(TC_CLEAN_FLAG);
			qValue.setValues(qValue.getRecordSize(),currTimestamp,m_cachingAlgorithm->GetDataSize());
			ret=m_qDB->Insert(qKey,qValue,partitionID);
			if(ret!=0)
			{
				if(ret==TC_FAILED_MEM_FULL)
				{
					if(EvictAndInsertQDBRec(qKey,qValue,partitionID,stats_update)!=0)
					{
						//m_logStream->print("TC::SyncInsert failed to insert QDB record even after victimizing\n");
						cacheFailure=true;
						goto failure;
					}
				}
				else{
					cacheFailure=true;
					goto failure;
				}
			}
			iQDB=true;

			stats_update.cleanBytes+=Value.get_size();
			stats_update.numQ+=1;
			stats_update.totalQ+=newQ.getFraction();

failure:
			if(cacheFailure)
			{
				if(iCacheDBK1)
				{
					if(m_cacheDB->DeleteDataRec(cacheKeyK1,partitionID)!=0)
						m_logStream->print( "TC::SyncInsert- Could not delete old Data Record from CacheDB\n" );
				}
				if(iCacheDBK2)
				{
					if(m_cacheDB->DeleteMDLookup(cacheKeyK2,partitionID)!=0)
						m_logStream->print( "TC::SyncInsert- Could not delete old MD lookup from CacheDB\n" );
				}
				if(iQDB)
				{
					if(m_qDB->Delete(qKey,partitionID)!=0)
						m_logStream->print( "TC::SyncInsert- Could not delete old record from QDB\n" );
				}
			}
		}
	}
		
	if(DataRecExistsInCache) 
	{
		if(qdbRecExists)
		{
			if(! (oldDirtyByte==TC_CLEAN_FLAG && oldQ==newQ)) 
			{
				qKey.setHeaderFlag(oldDirtyByte);
				qKey.setQValue(oldQ);
				if(m_qDB->Delete(qKey,partitionID)!=0)
					m_logStream->print( "TC::SyncInsert- Could not delete old record from QDB\n" );
				else 
				{
					if(oldDirtyByte==TC_CLEAN_FLAG)
						stats_update.cleanBytes-=size;
					else if(oldDirtyByte==TC_DIRTY_FLAG) {
						stats_update.dirtyBytes-=size;
					}
					
				}
			}
		}
		if(!admitKey || !DoAdmissionControl)
		{
			if(m_cacheDB->DeleteDataRec(cacheKeyK1,partitionID)!=0)
					m_logStream->print( "TC::SyncInsert- Could not delete old Data Record from CacheDB\n" );
			if(m_cacheDB->DeleteMDLookup(cacheKeyK2,partitionID)!=0)
					m_logStream->print( "TC::SyncInsert- Could not delete old MD lookup from CacheDB\n" );
			
		}	
		stats_update.totalQ-=oldQ.getFraction();
		stats_update.numQ--;
		
	}

	m_contextManager->ReleaseContext(cs);
	if(diskInsert)
		ret=m_diskDB->Put(dzname,Key,Value,&stats_update);
	
	m_lock->release(dzname,Key);
	
	if(ret!=0)
		m_logStream->print( "TC::SyncInsert- Could not insert record ret "+ ITS(ret)+"\n" );
	m_statsManager->ModifyNumSynchronousInsert(-1);
	UpdateStats(stats_update,partitionID);

	long long stats_dirtyBytes=0;
	long long stats_cleanBytes=0;
	long long totalBytes=0;
	double dirty_clean_ratio=0;

	//Check if dirty/ clean bytes is less 20%
	stats_dirtyBytes=m_statsManager->GetDirtyBytes(partitionID);

	stats_cleanBytes=m_statsManager->GetCleanBytes(partitionID);
	if (L==0)
		totalBytes=m_cacheSize/m_numPartitions;
	else {
		totalBytes=stats_dirtyBytes+stats_cleanBytes;
	}



	dirty_clean_ratio= (double)stats_dirtyBytes/totalBytes;

	bool mode=(m_statsManager->GetPartitionMode(partitionID));

	if( dirty_clean_ratio < m_asynchronousToSynchronousRatio && !mode )
	{
		//m_logStream->print("Background Thread::Switched to asynchronous Insert Partition id " +ITS(partitionID)+"\n");
		m_statsManager->SetPartitionMode(partitionID,true);
	}

	return ret;
}
