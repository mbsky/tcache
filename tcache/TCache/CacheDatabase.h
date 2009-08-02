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
#pragma once

#include "Common.h"
#include <iostream>
#include "ConfigParameters.h"
#include "CachingAlgorithm.h"
#include "ContextManager.h"
#include "StatisticsManager.h"
#include "TCQValue.h"

using namespace std;

class _export CacheKey
{
private:
	char	m_header;
	uchar	m_dzNameSize;
	char*	m_dzName;
	char	m_separator;
	uchar	m_keySize;
	char*	m_key;
	
	ContextStructure*	m_context_struct;

public:
	//takes the context structure to initialize the resources inside
	CacheKey( ContextStructure*	context_struct );
	~CacheKey();

	//Use internally to set up the correponding header for the key
	void setDataRecHeader( void ){ m_header = TC_CACHE_DATA_REC_KEY_HEADER; };
	
	//Use internally to set up the correponding header for the key
	void setMDHeader( void ){ m_header = TC_CACHE_MD_LOOKUP_KEY_HEADER; };

	//Takes a data block and initilize the internal attributes
	void setValues ( void* data, const int& data_size  );

	//Initilize the internal attributes
	void setValues ( const Vdt& DZName, const Vdt& Key );

	//Initilize the internal attributes
	void setValues ( const char& headerFlag, const Vdt& DZName, const Vdt& Key );

	char getHeaderFlag(){ return m_header; };

	Vdt getDzName() const { return Vdt( m_dzName, m_dzNameSize ); }

	Vdt getKey() const { return Vdt( m_key, m_keySize ); }

	void getData( char* &data, int& size );	

	ContextStructure* getContextStruct() { return m_context_struct; };

};

class _export CacheValue
{
	private:
	char	m_dirtyBit;
	TCQValue	m_QValue;

	ContextStructure*	m_context_struct;

	void* m_dataBuf;
	int m_dataBuf_size;

public:
	//takes the context structure to initialize the resources inside
	CacheValue( ContextStructure*	m_context_struct );
	~CacheValue();

	//void setValues ( const void* data, const int& data_size  );

	//Initilize internal attributes. Set up internal pointer to point to void* data argument
	void setDataRecValues ( const Vdt& Value );

	//Initilize internal attributes. Set up internal pointer to point to void* data argument
	void setDataRecValues ( void* data, const int& data_size  );

	//Use internally to update attribute m_dataBuf_size
	void setDataRecSize ( const int& data_size  ){ m_dataBuf_size = data_size; };

	//Takes a data block and initilize the internal attributes for MD
	void setMDValues ( void* data, const int& data_size );

	//Set internal attributes
	void setMDValues ( const char& dirtybit, const TCQValue& QValue );

	//Set a void* data pointer to internal m_dataBuf
	void getDataRec( void* &data, int& size );	

	//Use internally to update attribute m_dataBuf_size
	int getDataRecSize ( ){ return m_dataBuf_size; };

	//Initilize the void* data pointer with internal attributes
	void getMD( void* &data, int& size );	

	char getDirtyBit() { return m_dirtyBit; };
	
	TCQValue getQValue() { return m_QValue; };

	ContextStructure* getContextStruct() { return m_context_struct; };
};



class _export CacheDatabase
{
private:
	int m_numberofpartitions;
	Db** m_bdbCacheArray;

	DbEnv* m_bdbMemEnv;

	// Retrieve Cahce Db handle(Db*) for the specified partition
	int		getDbp( const int& partition_ID, Db* &ret_dbp );

public:
	CacheDatabase(void);
	~CacheDatabase(void);

	//Initialize Cache
	int Initialize( DbEnv* memoryDbEnv, const ConfigParametersV& cp );

	//Shutdown Cache
	int Shutdown ( void );

	//Insert Data records in Cache
	int InsertDataRec(  CacheKey& Key,  CacheValue& Value, const int partition_ID, stats* stats_update = NULL );

	//Insert Metadata in Cache
	int InsertMDLookup(  CacheKey& Key,  CacheValue& Value, const int partition_ID, stats* stats_update = NULL );

	//Get Data Record from Cache
	int GetDataRec(  CacheKey& Key,  CacheValue& Value, const int partition_ID, stats* stats_update = NULL );

	//Get Data Record from Cache
	int GetDataRec(  CacheKey& Key, Dbt* dbtValue, const int partition_ID, stats* stats_update = NULL );

	//Get Metadata from Cache
	int GetMDLookup(  CacheKey& Key,  CacheValue& Value, const int partition_ID, stats* stats_update = NULL );

	//Delete Data Record from Cache
	int DeleteDataRec(  CacheKey& Key, const int partition_ID, stats* stats_update = NULL );
	
	//Delete Metadata from Cache
	int DeleteMDLookup(  CacheKey& Key, const int partition_ID, stats* stats_update = NULL );

	// Check if key exists in cache returns true if it exists otherwise false
	bool Exists (CacheKey& Key, const int partition_ID, stats* stats_update = NULL );

	int GetDBCursor( Dbc* &cursorp, DbTxn* &txn, const int& partition_ID, stats* stats_update );

	void printContents(	const int& partition_ID, ContextStructure* c_struct, std::ostream& out );
};
