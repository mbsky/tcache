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
#include "TCache.h"


#define CHECK(bool_value) if(bool_value)	{\
						if(ret!=0)\
							return false;\
					}\
					else {\
						if(ret==0)\
						return false;\
					}

int	TCache::runInternalTests()
{
	int ret = 0;
	//ret |= testEvictions();
	
	
	
	//Create some hardcoded dzname and key
	char *dzname="B";
	Vdt vdt_dzname(dzname,sizeof(char));

	int key=4;
	Vdt vdt_key((char *)&key,sizeof(int));
	const int size=10;
	char *value=new char[size];
	*(int *)value=key;
	Vdt vdt_value(value,size);
	
	
	// The test have to run mutually exclusive beacuase I am using the same data item.

	// Test 1 no cacheDB K2 record 
	/*ret=InsertNoCache(vdt_dzname,vdt_key,vdt_value,false,true,false,false);

	//Added a sleep so that the background thread is done with it
	Sleep(4000);

	if(ExistsInCache(vdt_dzname, vdt_key, vdt_value,true,true,true,true))
		printf("Test 1 Passed\n");
	else
		printf("Test 1 failed\n");*/

	
	//Test 2 no cacheDB K1 record 
	/*ret=InsertNoCache(vdt_dzname,vdt_key,vdt_value,true,false,false,false);

	//Added a sleep so that the background thread is done with it
	Sleep(4000);

	if(ExistsInCache(vdt_dzname, vdt_key, vdt_value,false,false,false,false))
		printf("Test 2 Passed\n");
	else
		printf("Test 2 failed\n");*/
	

	//Test 3 Cache Clean and QDB dirty
	/*ret=InsertNoCache(vdt_dzname,vdt_key,vdt_value,false,false,true,false);
	//Added a sleep so that the background thread is done with it
	Sleep(4000);

	if(ExistsInCache(vdt_dzname, vdt_key, vdt_value,true,true,false,false))
		printf("Test 3 Passed\n");
	else
		printf("Test 3 failed\n");*/

	// Test 4 fails, the background thread does not get the cpu,context switch not happenind :S:S 
	//slept for 32 seconds.
	// Insert an addition QDB record Dirty Q 
	ret=InsertNoCache(vdt_dzname,vdt_key,vdt_value,false,false,false,true);
	//Added a sleep so that the background thread is done with it
	Sleep(16000);

	if(ExistsInCache(vdt_dzname, vdt_key, vdt_value,true,true,true,true))
		printf("Test 4 Passed\n");
	else
		printf("Test 4 failed\n");
	return ret;
}

int TCache::testEvictions()
{
	ContextStructure* c_struct;
	if( m_contextManager->GetFreeContext( c_struct ) != 0 )
		m_logStream->print( "Error. Could not get free context\n" );

	QDBKey qKey( c_struct );
	QDBValue qValue( c_struct );

	//m_qDB->Insert( qKey, qValue, 0 );
	
	//Delete( );
	m_contextManager->ReleaseContext( c_struct );
	return -1;
}

// NoCacheDBK1 = true if no data rec 
// NoCacheDBK2 = true if no metadata lookup rec 
// CacheK2CleanDirtyQDB =true Insert the Cache DBK2 as clean and QDB as dirty 
// additionalQDBRec=true Insert an additional QDB record
int TCache::InsertNoCache(const Vdt& dzname, const Vdt& Key, const Vdt& Value,bool noCacheDBK1,bool noCacheDBK2,bool cacheK2CleanDirtyQDB, bool additionalQDBRec)
{
	ContextStructure* cs;
	m_contextManager->GetFreeContext( cs );

	int partitionID = TCUtility::bernsteinHash(dzname, Key, m_numPartitions);
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter(&currTimestamp);

	int ret = 0;
		
	char D=TC_DIRTY_FLAG;
	double newQ = 1.0/Value.get_size();
	

	CacheKey cacheKeyK2(cs);
	CacheValue cacheValueK2(cs);
	cacheKeyK2.setValues(dzname, Key);
	
	CacheKey cacheKeyK1 (cs);
	CacheValue cacheValueK1(cs);
	cacheKeyK1.setValues(dzname,Key);
	cacheValueK1.setDataRecValues(Value);

	
	QDBKey qKey(cs);
	QDBValue qValue(cs);
	qKey.setValues(D,newQ,dzname,Key);
	

	long long dirtyBytes=0;

	if(!noCacheDBK1)
		ret= m_cacheDB->InsertDataRec(cacheKeyK1,cacheValueK1,partitionID);
	
	dirtyBytes+=Value.get_size();

	char cacheDB_DirtyByte=TC_DIRTY_FLAG;
	if(cacheK2CleanDirtyQDB)
		cacheDB_DirtyByte=TC_CLEAN_FLAG ;
	cacheValueK2.setMDValues(cacheDB_DirtyByte,newQ);
	
	if(!noCacheDBK2)
		ret= m_cacheDB->InsertMDLookup(cacheKeyK2,cacheValueK2,partitionID);
	
	qKey.setQValue(newQ);
	qKey.setHeaderFlag(TC_DIRTY_FLAG);
	qValue.setValues(Value.get_size(),currTimestamp,m_cachingAlgorithm->GetDataSize());
	ret=m_qDB->Insert(qKey,qValue,partitionID);
	
	if(additionalQDBRec)
	{
		qKey.setQValue(0.001);
		qKey.setHeaderFlag(TC_DIRTY_FLAG);
		qValue.setValues(1000,currTimestamp,m_cachingAlgorithm->GetDataSize());
		ret=m_qDB->Insert(qKey,qValue,partitionID);
	}

	m_contextManager->ReleaseContext(cs);
	
	m_statsManager->IncreaseDirtyBytes(partitionID,dirtyBytes);
	
	//Signal the semaphore
	if (!ReleaseSemaphore(m_semaphore,  // handle to semaphore
							1,                      // increase count by one
							NULL))            // not interested in previous count
	{
		m_logStream->print("TC::AsyncInsert failed to signal semaphore\n"); 
		return -1;
	}

	return 0;	

}
// The boolean specify  what sould exist
bool TCache::ExistsInCache(const Vdt& dzname, const Vdt& Key, Vdt& Value,bool eCacheDBK1,bool eCacheDBK2,bool eQDB, bool eDisk)
{
	ContextStructure* cs;
	m_contextManager->GetFreeContext( cs );

	int partitionID = TCUtility::bernsteinHash(dzname, Key, m_numPartitions);
	LARGE_INTEGER currTimestamp;
	QueryPerformanceCounter(&currTimestamp);

	int ret = 0;
		
	char D=TC_CLEAN_FLAG;
	double newQ = 1.0/Value.get_size();
	

	CacheKey cacheKeyK2(cs);
	CacheValue cacheValueK2(cs);
	cacheKeyK2.setValues(dzname, Key);
	
	CacheKey cacheKeyK1 (cs);
	CacheValue cacheValueK1(cs);
	cacheKeyK1.setValues(dzname,Key);
	cacheValueK1.setDataRecValues(Value);

	
	QDBKey qKey(cs);
	QDBValue qValue(cs);
	qKey.setValues(D,newQ,dzname,Key);


	ret=m_cacheDB->GetDataRec(cacheKeyK1,cacheValueK1,partitionID);
	CHECK(eCacheDBK1)
	
	
	ret=m_cacheDB->GetMDLookup(cacheKeyK1,cacheValueK2,partitionID);
	CHECK(eCacheDBK2)

	ret=m_qDB->Get(qKey,qValue,partitionID);
	CHECK(eQDB)

	ret=m_diskDB->Get(dzname,Key,&Value);
	CHECK(eDisk)
	m_contextManager->ReleaseContext(cs);
	return true;
}