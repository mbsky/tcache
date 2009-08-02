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

#ifndef __TC_Q_DATABASE_H__
#define __TC_Q_DATABASE_H__

#include "Common.h"
#include <iostream>
#include "ConfigParameters.h"
#include "CachingAlgorithm.h"
#include "ContextManager.h"
#include "StatisticsManager.h"
#include "TCQValue.h"

class _export QDBKey
{
private:
	char	m_headerFlag;
	TCQValue	m_qValue;
	uchar	m_dzNameSize;
	char*	m_dzName;
	uchar	m_keySize;
	char*	m_key;

	ContextStructure*	m_context_struct;

	char*	m_dataBuffer;
	int		m_dataBufferSize;
	int		m_storedDataSize;		
	bool	m_selfAllocated;
	bool	m_bufferHoldsData;

public:
	QDBKey();
	QDBKey( const int& buffer_size );
	QDBKey( ContextStructure* c_struct );
	~QDBKey();

	void setValues( const char& headerFlag, 
					const TCQValue& qValue, 
					const Vdt& dzName,
					const Vdt& key );

	// Sets the internal values based on a pointer to a block of data generated 
	//  in the same format as specified in getData()
	// Typically, this will be used when a Dbt is retrieved from BerkeleyDB and
	//  you want to generate the corresponding QDBKey.
	void setValues( const void* data_ptr, const int& data_size );	
	void setValues( const void* data_ptr, const int& data_size, const bool& copyToInternalBuffer );

	// Set individual components
	void setQValue( const TCQValue& q_value ); 
	void setHeaderFlag( const char& headerFlag ); 

	// Get individual components
	char getHeaderFlag() const { return m_headerFlag; }
	TCQValue getQValue() const { return m_qValue; }
	Vdt getDzName() const { return Vdt( m_dzName, m_dzNameSize ); }
	Vdt getKey() const { return Vdt( m_key, m_keySize ); }
	uint getDataBufferSize() const; 

	// Assembles the parts of the key as a contiguous block of memory and sets the
	//  data pointer to that memory location. Size of actual data is also returned.
	void getData( char* &data, int& size );	

	int compare( const QDBKey& other_key );

	ContextStructure* getContextStruct() { return m_context_struct; };
};


class _export QDBValue
{
private:
	int		m_recordSize;
	LARGE_INTEGER	m_lastUpdateTimestamp;
	//void*	m_metadata;
	int		m_metadataSize;
	int		m_offsetLength;

	ContextStructure*	m_context_struct;

public:
	QDBValue( ContextStructure* c_struct );
	~QDBValue();

	void setValues( const int& record_size,
					const LARGE_INTEGER& updateTimestamp,
					//void* metadata,
					const int& metadata_size );

	// Sets the internal values based on a pointer to a block of data generated 
	//  in the same format as specified in getData()
	// Typically, this will be used when a Dbt is retrieved from BerkeleyDB and
	//  you want to generate the corresponding QDBValue.
	void setValues( const void* data_ptr, const int& data_size );

	// Set individual components
	void setRecordSize( const int& size ) { m_recordSize = size; }

	// Get individual components
	uint getRecordSize() const { return m_recordSize; }
	LARGE_INTEGER getUpdateTimestamp() const { return m_lastUpdateTimestamp; }
	void* getMetadataPtr() const { return m_context_struct->qdb_value + m_offsetLength; }

	int	 checkMetadataSize( const int& required_size );
	ContextStructure* getContextStructure() { return m_context_struct; }

	// Assembles the parts of the key as a contiguous block of memory and sets the
	//  data pointer to that memory location. Size of actual data is also returned.
	void getData( char* &data, int& size );
};


class _export QDatabase
{
private:
	int		m_numPartitions;

	// Array of Db* elements. Size of array is based on number of partitions
	Db**	m_dbArray;	

	DbEnv*	m_memoryDbEnv;

public:	// TODO: change this back to private after debugging is done
	// Retrieve Db handle(Db*) for the specified partition
	int		getDbp( const int& partition_ID, Db* &ret_dbp );


public:
	QDatabase();
	~QDatabase();

	int Initialize( DbEnv* memoryDbEnv, const ConfigParametersV& cp );
	int Shutdown();

	int Insert( QDBKey& key, QDBValue& value, const int& partition_ID, stats* stats_update = NULL );
	int Insert( QDBKey& key, Dbt* dbtValue, const int& partition_ID, stats* stats_update = NULL );
	int Get( QDBKey& key, QDBValue& value, const int& partition_ID, stats* stats_update = NULL );
	int Delete( QDBKey& key, const int& partition_ID, stats* stats_update = NULL );
	int GetDBCursor( Dbc* &cursorp, DbTxn* &txn, const int& partition_ID, stats* stats_update = NULL );
	int GetLowestDirtyRecord( QDBKey& key, QDBValue& value, const int& partition_ID, stats* stats_update = NULL );


	//// Debugging functions

	// Dumps the entire contents of the Q_DB out to the specified output stream
	// Not Thread Safe
	void printContents( const CachingAlgorithm* cachingAlgorithm, 
						const int& partition_ID,
						ContextStructure* c_struct,
						std::ostream& out );

};

#endif // __TC_Q_DATABASE_H__
