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

#include "StdAfx.h"
#include <cstring>
#include "QDatabase.h"


//
// QDBKey definitions
// ==================
QDBKey::QDBKey()
{
	m_selfAllocated = false;
	m_bufferHoldsData = false;
	m_context_struct = NULL;
	m_dataBufferSize = 0;
	m_dataBuffer = NULL;
}

QDBKey::QDBKey( const int& buffer_size )
{
	m_selfAllocated = true;
	m_bufferHoldsData = false;
	m_context_struct = NULL;
	m_dataBufferSize = buffer_size;
	m_dataBuffer = new char[m_dataBufferSize];
}

QDBKey::QDBKey( ContextStructure* c_struct )
{
	m_selfAllocated = false;
	m_bufferHoldsData = false;
	m_context_struct = c_struct;
}

QDBKey::~QDBKey()
{
	if( m_selfAllocated )
		delete [] m_dataBuffer;
}

void QDBKey::setValues( const char& headerFlag, 
						const TCQValue& qValue, 
						const Vdt& dzName,
						const Vdt& key )
{
	m_headerFlag = headerFlag;
	m_qValue = qValue;
	m_dzName = dzName.get_data();
	m_dzNameSize = (uchar)dzName.get_size();
	m_key = key.get_data();
	m_keySize = (uchar)key.get_size();
	m_bufferHoldsData = false;
}

void QDBKey::setValues( const void* data_ptr, const int& data_size )
{	
	setValues( data_ptr, data_size, false );
}

// When copyToInternalBuffer is set to true, it incurs 2 additional memcopies to
//  keep an internal copy of the key and dzName
void QDBKey::setValues( const void* data_ptr, const int& data_size, const bool& copyToInternalBuffer )
{
	char* cptr = (char*)data_ptr;
	char* cptr_internal;

	// Keep track of whether internal buffer is being used to store the values
	m_bufferHoldsData = copyToInternalBuffer;

	if( copyToInternalBuffer )
	{
		if( m_selfAllocated )
		{
			// Use the internal char array memory
			if( data_size > m_dataBufferSize )
			{
				ContextManager::ResizeMemory( m_dataBuffer, data_size );
				m_dataBufferSize = data_size;
			}
			cptr_internal = m_dataBuffer;
		}
		else
		{
			// Use the context structure memory
			if( data_size > m_context_struct->qdb_key_size )
			{
				ContextManager::ResizeMemory( m_context_struct->qdb_key, data_size );
				m_context_struct->qdb_key_size = data_size;
			}
			cptr_internal = m_context_struct->qdb_key;
		}

		// Make a copy of the data
		m_storedDataSize = data_size;
		memcpy( cptr_internal, data_ptr, data_size );
		cptr = cptr_internal;
	}

	m_headerFlag = *cptr;
	cptr += sizeof(m_headerFlag);
	
	m_qValue = *(TCQValue*) cptr;
	cptr += sizeof(m_qValue);

	m_dzNameSize = *(uchar*) cptr;
	cptr += sizeof(m_dzNameSize);

	m_dzName = cptr;
	cptr += m_dzNameSize;

	m_keySize = *(uchar*) cptr;
	cptr += sizeof(m_keySize);

	m_key = cptr;
}


void QDBKey::getData( char* &data, int& size )
{	
	if( m_bufferHoldsData )
	{
		// TODO: potential danger if a user is allowed to change the size of the internal values
		size = m_storedDataSize;

		if( m_selfAllocated )
			data = m_dataBuffer;
		else
			data = m_context_struct->qdb_key;

		return;
	}

	int length = 0;
	size = 0;

	// Obtain the total size first. This is to check if our buffer is large enough
	size += sizeof( m_headerFlag );
	size += sizeof( m_qValue );
	size += sizeof( m_dzNameSize );
	size += m_dzNameSize;
	size += sizeof( m_keySize );
	size += m_keySize;	

	char* cptr;

	if( m_selfAllocated )
	{
		// Resize buffer if it is too small
		if( size > m_dataBufferSize )
		{
			ContextManager::ResizeMemory( m_dataBuffer, size );
			m_dataBufferSize = size;
		}

		cptr = m_dataBuffer;

		// Return pointer to the buffer location
		data = m_dataBuffer;
	}
	else
	{	
		if( m_context_struct == NULL )
		{
			data = NULL;
			size = -1;
			std::cout<< "QDBKey Error: No Internal buffer specifed. getData() is not allowed\n";
			return;
		}

		// Resize buffer if it is too small
		if( size > m_context_struct->qdb_key_size )
		{
			ContextManager::ResizeMemory( m_context_struct->qdb_key, size );
			m_context_struct->qdb_key_size = size;
		}

		cptr = m_context_struct->qdb_key;

		// Return pointer to the buffer location
		data = m_context_struct->qdb_key;		
	}

	length = sizeof( m_headerFlag );
	memcpy( cptr, &m_headerFlag, length );
	cptr += length;

	length = sizeof( m_qValue );
	memcpy( cptr, &m_qValue, length );
	cptr += length;

	length = sizeof( m_dzNameSize );
	memcpy( cptr, &m_dzNameSize, length );
	cptr += length;

	length = m_dzNameSize;
	memcpy( cptr, m_dzName, length );
	cptr += length;

	length = sizeof( m_keySize );
	memcpy( cptr, &m_keySize, length );
	cptr += length;

	length = m_keySize;
	memcpy( cptr, m_key, length );
	cptr += length;
}

uint QDBKey::getDataBufferSize() const
{
	if( m_selfAllocated )
		return m_dataBufferSize;
	else
		return m_context_struct->qdb_key_size;
}

void QDBKey::setHeaderFlag( const char &headerFlag )
{
	m_headerFlag = headerFlag;

	// Need to modify the data in the internal copy
	if( m_bufferHoldsData )
	{
		char* cptr;

		if( m_selfAllocated )
			cptr = m_dataBuffer;
		else
			cptr = m_context_struct->qdb_key;

		*cptr = headerFlag;
	}
}

void QDBKey::setQValue( const TCQValue &q_value )
{
	m_qValue = q_value;

	// Need to modify the data in the internal copy
	if( m_bufferHoldsData )
	{
		char* cptr;

		if( m_selfAllocated )
			cptr = m_dataBuffer;
		else
			cptr = m_context_struct->qdb_key;

		cptr += sizeof(m_headerFlag);
		*(TCQValue*)cptr = q_value;
	}
}

int QDBKey::compare( const QDBKey& other_key )
{
	// Returns: 
    // < 0 if this < other
    // = 0 if this = other
    // > 0 if this > other

	if( m_headerFlag < other_key.m_headerFlag )
		return -1;
	else if( m_headerFlag > other_key.m_headerFlag )
		return 1;

	if( m_qValue < other_key.m_qValue )
		return -1;
	else if( m_qValue > other_key.m_qValue )
		return 1;

	if( m_dzNameSize < other_key.m_dzNameSize )
		return -1;
	else if( m_dzNameSize > other_key.m_dzNameSize )
		return 1;

	int ret = memcmp( m_dzName, other_key.m_dzName, m_dzNameSize );
	if( ret != 0 )
		return ret;

	if( m_keySize < other_key.m_keySize )
		return -1;
	else if( m_keySize > other_key.m_keySize )
		return 1;

	ret = memcmp( m_key, other_key.m_key, m_keySize );
	return ret;
}



//
// QDBValue definitions
// ====================


QDBValue::QDBValue( ContextStructure* c_struct )
{
	m_context_struct = c_struct;
	m_offsetLength = sizeof( m_recordSize ) + sizeof( m_lastUpdateTimestamp );
}

QDBValue::~QDBValue()
{
}

void QDBValue::setValues(	const int& record_size,
							const LARGE_INTEGER& updateTimestamp,
							const int& metadata_size )
{
	m_recordSize = record_size;
	m_lastUpdateTimestamp = updateTimestamp;
	
	// Metadata is assumed to be stored at some offset of the
	//  Context Structure qdb_value buffer up to the metadata_size
	m_metadataSize = metadata_size;
}

void QDBValue::setValues( const void* data_ptr, const int& data_size )
{
	char* cptr = (char*)data_ptr;
	int size_remaining = data_size;

	if( checkMetadataSize( data_size ) != 0 )
	{
		std::cout<< "Error. Buffer is not large enough\n";
	}

	m_recordSize = *(int*) cptr;
	cptr += sizeof(m_recordSize);
	size_remaining -= sizeof(m_recordSize);

	m_lastUpdateTimestamp = *(LARGE_INTEGER*) cptr;
	cptr += sizeof(m_lastUpdateTimestamp);
	size_remaining -= sizeof(m_lastUpdateTimestamp);

	m_metadataSize = size_remaining;

	void* metadata = getMetadataPtr();
	memcpy( metadata, cptr, m_metadataSize );	
}

int	 QDBValue::checkMetadataSize( const int& required_size )
{
	int new_size = required_size + m_offsetLength;
	if( new_size > m_context_struct->qdb_value_size )
	{
		if( ContextManager::ResizeMemory( m_context_struct->qdb_value, new_size ) == 0 )
		{
			m_context_struct->qdb_value_size = new_size;
		}
		else
		{
			// Could not resize memory
			return -1;
		}
	}

	// Metadata buffer is at least large enough
	return 0;
}

void QDBValue::getData( char* &data, int& size )
{
	int length = 0;
	size = 0;

	// Obtain the total size first. This is to check if our buffer is large enough
	size += sizeof( m_recordSize );
	size += sizeof( m_lastUpdateTimestamp );	
	size += m_metadataSize;
	
	// Resize buffer if it is too small
	if( size > m_context_struct->qdb_value_size )
	{
		// The buffer contains the metadata, so we need to realloc
		//  so we don't lose that data
		ContextManager::ReallocMemory(	m_context_struct->qdb_value,
										m_context_struct->qdb_value_size,
										size );
		m_context_struct->qdb_value_size = size;
	}

	char* cptr = m_context_struct->qdb_value;
	
	length = sizeof( m_recordSize );
	memcpy( cptr, &m_recordSize, length );
	cptr += length;	

	length = sizeof( m_lastUpdateTimestamp );
	memcpy( cptr, &m_lastUpdateTimestamp, length );
	cptr += length;	

	// Metadata is already stored so no need to copy
	length = m_metadataSize;
	cptr += length;

	// Return pointer to the buffer location
	data = m_context_struct->qdb_value;
}


//
// QDatabase definitions
// =====================


// BerkeleyDB B-tree comparison function
int compare_Q_value( Db *dbp, const Dbt *a, const Dbt *b )
{
	QDBKey qdbkey_a, qdbkey_b;

	qdbkey_a.setValues( a->get_data(), a->get_size() );
	qdbkey_b.setValues( b->get_data(), a->get_size() );

	// Returns: 
    // < 0 if a < b 
    // = 0 if a = b 
    // > 0 if a > b

	return qdbkey_a.compare( qdbkey_b );
}


QDatabase::QDatabase()
{
	
}

QDatabase::~QDatabase()
{

}

int QDatabase::Initialize( DbEnv* memoryDbEnv, const ConfigParametersV& cp )
{
	int ret = -1;
	std::string temp_dbname;
	Db* dbp;
	DbMpoolFile *mpf = NULL;

	m_numPartitions = cp.MemBDB.DatabaseConfigs.Partitions;

	m_dbArray = new Db*[m_numPartitions];

	m_memoryDbEnv = memoryDbEnv;

	for( int i = 0; i < m_numPartitions; i++ )
	{
		temp_dbname = TC_ORDERED_QVALS_DBNAME;
		temp_dbname += TCUtility::intToString( i );

		dbp = new Db( m_memoryDbEnv, 0 );

		dbp->set_bt_compare( compare_Q_value );

		if( cp.MemBDB.DatabaseConfigs.QDBPageSize > 0 )
			dbp->set_pagesize( cp.MemBDB.DatabaseConfigs.QDBPageSize );
		
		ret = dbp->open(	NULL,
							NULL,
							temp_dbname.c_str(),
							DB_BTREE,
							DB_CREATE | DB_AUTO_COMMIT,
							0 );
		if( ret != 0 )
			std::cout<< "QDatabase: Error. Could not open main memory database handle\n";

		mpf = dbp->get_mpf();
		ret = mpf->set_flags( DB_MPOOL_NOFILE, 1 );
		if( ret != 0 )
			std::cout<< "QDatabase: Error. Could not set DbMpoolFile\n";

		m_dbArray[i] = dbp;
	}

	return ret;
}

int QDatabase::Shutdown()
{
	int ret = -1;
	for( int i = 0; i < m_numPartitions; i++ )
	{
		ret = m_dbArray[i]->close( 0 );
		if( ret != 0 )
		{
			// Error closing database
			std::cout<< "QDatabase: Error closing database\n";
		}

		delete m_dbArray[i];
	}

	delete [] m_dbArray;
	m_dbArray = NULL;

	return 0;
}


int	QDatabase::getDbp( const int& partition_ID, Db* &ret_dbp )
{
	if( partition_ID < 0 || partition_ID >= m_numPartitions )
		return -1;

	ret_dbp = m_dbArray[partition_ID];
	return 0;
}

int QDatabase::Insert( QDBKey& key, QDBValue& value, const int& partition_ID, stats* stats_update )
{
	char* value_ptr;
	int value_size;
	value.getData( value_ptr, value_size );

	// Convert value to a Dbt
	Dbt dbtValue( value_ptr, value_size );
	dbtValue.set_ulen( value_size );
	dbtValue.set_flags( DB_DBT_USERMEM );

	return Insert( key, &dbtValue, partition_ID, stats_update );
}

int QDatabase::Insert( QDBKey& key, Dbt* dbtValue, const int& partition_ID, stats* stats_update )
{
	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( getDbp( partition_ID, dbp ) != 0 )
	{		
		std::cout<< "QDatabase: Error. Could not get open db handle. Check partition_ID\n";
	}

	// Get and convert the key to a Dbt
	char* key_ptr;
	int key_size;
	key.getData( key_ptr, key_size );

	Dbt dbtKey( key_ptr, key_size );
	dbtKey.set_ulen( key_size );
	dbtKey.set_flags( DB_DBT_USERMEM );

	// Try to insert into BerkeleyDB
	int ret;
	DbTxn* txn = NULL;

	int numTries = 0;
	bool keepTrying = true;
	while( keepTrying && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		m_memoryDbEnv->txn_begin( NULL, &txn, 0 );
		numTries++;

		try
		{
			ret = dbp->put( txn, &dbtKey, dbtValue, 0 );

			txn->commit( 0 );
			keepTrying = false;
		}
		catch( DbException& e )
		{
			//std::cout<< "QDB::Insert: " << e.what() << " Try #" << numTries << std::endl;

			if( e.get_errno() == DB_LOCK_DEADLOCK )
			{
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{				
				if( e.get_errno() == BDB_FAILED_MEM_FULL )
				{
					ret = TC_FAILED_MEM_FULL;
					//cout<< ex.what() << endl;
				}
				keepTrying = false;
			}

			txn->abort();
		}
	}

	// The insert was still deadlocked after retrying multiple times
	if( keepTrying )
	{
		ret = -1;
	}

	return ret;
}

int QDatabase::Get( QDBKey& key, QDBValue& value, const int& partition_ID, stats* stats_update )
{
	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( getDbp( partition_ID, dbp ) != 0 )
	{
		std::cout<< "QDatabase: Error. Could not get open db handle. Check partition_ID\n";
	}

	// Get and convert the key to a Dbt
	char* key_ptr;
	int key_size;
	key.getData( key_ptr, key_size );

	Dbt dbtKey( key_ptr, key_size );
	dbtKey.set_ulen( key_size );
	dbtKey.set_flags( DB_DBT_USERMEM );

	// Set up the value as a dbt using the memory buffer from the QDBValue's ContextStructure
	ContextStructure* c_struct = value.getContextStructure();
	Dbt dbtValue( c_struct->qdb_value, c_struct->qdb_value_size );
	dbtValue.set_ulen( c_struct->qdb_value_size );
	dbtValue.set_flags( DB_DBT_USERMEM );

	// Try to get from BerkeleyDB
	int ret;
	DbTxn* txn = NULL;
	

	int numTries = 0;
	bool keepTrying = true;
	while( keepTrying && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		m_memoryDbEnv->txn_begin( NULL, &txn, 0 );
		numTries++;

		try
		{
			ret = dbp->get( txn, &dbtKey, &dbtValue, 0 );
			txn->commit( 0 );

			// If get was successful, store the return value 
			if( ret == 0 )
				value.setValues( dbtValue.get_data(), dbtValue.get_size() );

			keepTrying = false;
		}
		catch( DbException& e )
		{
			//std::cout<< "QDB::Get: " << e.what() << " Try #" << numTries << std::endl;

			// Resizes the value buffer if it is too small
			if( e.get_errno() == DB_BUFFER_SMALL )
			{
				if( dbtValue.get_size() > dbtValue.get_ulen() )
				{
					// Resize the value
					ContextManager::ResizeMemory( c_struct->qdb_value, dbtValue.get_size() );
					c_struct->qdb_value_size = dbtValue.get_size();

					dbtValue.set_data( c_struct->qdb_value );
					dbtValue.set_ulen( c_struct->qdb_value_size );
				}
				else if( dbtKey.get_size() > dbtKey.get_ulen() )
				{
					// Resize the key
					ContextManager::ResizeMemory( c_struct->qdb_key, dbtKey.get_size() );
					c_struct->qdb_key_size = dbtKey.get_size();

					dbtKey.set_data( c_struct->qdb_key );
					dbtKey.set_ulen( c_struct->qdb_key_size );
				}
				else
				{
					std::cout<< "QDB::Get - Error. Neither the key nor value sizes are too small.\n";
				}
			}
			else if( e.get_errno() == DB_LOCK_DEADLOCK )
			{
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				keepTrying = false;
				ret = -1;
			}

			txn->abort();
		}
	}

	if( keepTrying )
	{
		ret = -1;
	}


	return ret;
}

int QDatabase::Delete( QDBKey& key, const int& partition_ID, stats* stats_update )
{
	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( getDbp( partition_ID, dbp ) != 0 )
	{
		std::cout<< "QDatabase: Error. Could not get open db handle. Check partition_ID\n";
	}

	int debug_int = *(int*)key.getKey().get_data();

	// Get and convert the key to a Dbt
	char* key_ptr;
	int key_size;
	key.getData( key_ptr, key_size );

	Dbt dbtKey( key_ptr, key_size );
	dbtKey.set_ulen( key_size );
	dbtKey.set_flags( DB_DBT_USERMEM );

	// Try to delete from BerkeleyDB
	int ret;
	DbTxn* txn = NULL;

	int numTries = 0;
	bool keepTrying = true;
	while( keepTrying && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		m_memoryDbEnv->txn_begin( NULL, &txn, 0 );
		numTries++;

		try
		{
			ret = dbp->del( txn, &dbtKey, 0 );

			txn->commit( 0 );
			keepTrying = false;
		}
		catch( DbException& e )
		{
			std::cout<< "QDB::Delete: " << e.what() << " Try #" << numTries << std::endl;

			if( e.get_errno() == DB_LOCK_DEADLOCK )
			{
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				keepTrying = false;
				ret = -1;
			}

			txn->abort();
		}
	}

	// The delete was still deadlocked after retrying multiple times
	if( keepTrying )
	{
		ret = -1;
	}

	return ret;
}

int QDatabase::GetDBCursor( Dbc* &cursorp, DbTxn* &txn, const int& partition_ID, stats* stats_update )
{
	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( getDbp( partition_ID, dbp ) != 0 )
	{
		std::cout<< "QDatabase: Error. Could not get open db handle. Check partition_ID\n";
	}

	int ret;
	m_memoryDbEnv->txn_begin( NULL, &txn, 0 );

	try
	{
		ret = dbp->cursor( txn, &cursorp, 0 );
	}
	catch( DbException& e )
	{		
		std::cout<< "QDB::GetCursor: " << e.what();
		ret = -1;
		txn->abort();
	}

	return ret;
}

int QDatabase::GetLowestDirtyRecord( QDBKey& key, QDBValue& value, const int& partition_ID, stats* stats_update )
{
	int ret = -1;

	char empty_char = 0;
	Vdt dzNameEmpty( &empty_char, sizeof(empty_char) );
	Vdt keyEmpty( &empty_char, sizeof(empty_char) );

	// Set the key to the beginning of the dirty records
	key.setValues( 'D', 0.0, dzNameEmpty, keyEmpty );

	char* keyData;
	int keySize;
	key.getData( keyData, keySize );

	ContextStructure* c_struct = value.getContextStructure();

	Dbt dbtKey( keyData, keySize );
	dbtKey.set_ulen( key.getDataBufferSize() );
	dbtKey.set_flags( DB_DBT_USERMEM );

	Dbt dbtValue( c_struct->qdb_value, 0 );
	dbtValue.set_ulen( c_struct->qdb_value_size );
	dbtValue.set_flags( DB_DBT_USERMEM );


	// Open the cursor and try to get the first dirty record 
	DbTxn* txn = NULL;
	Dbc* cursorp = NULL;

	// Keep trying to account for the buffer size being too small. 
	// Buffer is resized and the get is retried.
	int numTries = 0;
	bool keepTrying = true;
	GetDBCursor( cursorp, txn, partition_ID );

	while( keepTrying && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{		
		numTries++;

		try
		{
			ret = cursorp->get( &dbtKey, &dbtValue, DB_SET_RANGE );
			keepTrying = false;
			cursorp->close();
			txn->commit( 0 );

			// If get was successful, check if its a valid dirty record
			if( ret == 0 )
			{
				key.setValues( dbtKey.get_data(), dbtKey.get_size() );

				if( key.getHeaderFlag() == 'D' )
				{					
					value.setValues( dbtValue.get_data(), dbtValue.get_size() );
				}
				else
				{
					// Assuming that there are no more dirty records
					ret = 1;	// TODO: create error code NO_DIRTY_RECORDS					
				}
			}
		}
		catch( DbException& e )
		{
			std::cout<< "QDB::GetLowestDirty: " << e.what() << " Try #" << numTries << std::endl;

			// Resizes the value buffer if it is too small
			if( e.get_errno() == DB_BUFFER_SMALL )
			{
				if( dbtValue.get_size() > dbtValue.get_ulen() )
				{
					// Resize the value
					ContextManager::ResizeMemory( c_struct->qdb_value, dbtValue.get_size() );
					c_struct->qdb_value_size = dbtValue.get_size();

					dbtValue.set_data( c_struct->qdb_value );
					dbtValue.set_ulen( c_struct->qdb_value_size );
				}
				else if( dbtKey.get_size() > dbtKey.get_ulen() )
				{
					// Resize the key
					ContextManager::ResizeMemory( c_struct->qdb_key, dbtKey.get_size() );
					c_struct->qdb_key_size = dbtKey.get_size();
				}
				else
				{
					std::cout<< "GetLowestDirtyRecord - Error. Neither the key nor value sizes are too small.\n";
				}				

				// Reset the key to point to before the first dirty record
				key.setValues( 'D', 0.0, dzNameEmpty, keyEmpty );
				key.getData( keyData, keySize );
				dbtKey.set_data( keyData );
				dbtKey.set_ulen( c_struct->qdb_key_size );
				dbtKey.set_flags( DB_DBT_USERMEM );
			}
			else if( e.get_errno() == DB_LOCK_DEADLOCK )
			{
				// Reset the key to point to before the first dirty record
				key.setValues( 'D', 0.0, dzNameEmpty, keyEmpty );
				key.getData( keyData, keySize );
				dbtKey.set_data( keyData );
				dbtKey.set_ulen( c_struct->qdb_key_size );
				dbtKey.set_flags( DB_DBT_USERMEM );

				// Re-open the cursor in the case of deadlock
				// Leaving the cursor open during a deadlock doesn't allow the
				//  deadlock to be resolved.
				cursorp->close();
				txn->abort();

				if( stats_update )
					stats_update->NumDeadlocks++;

				GetDBCursor( cursorp, txn, partition_ID );
			}
			else
			{				
				keepTrying = false;
				cursorp->close();
				txn->abort();
			}
		}
	}

	// If the operation was still deadlocked after retrying a bunch of times, need
	//  to close up the cursor
	if( keepTrying )
	{
		cursorp->close();
		txn->abort();
	}

	return ret;
}

void QDatabase::printContents(	const CachingAlgorithm* cachingAlgorithm, 
								const int& partition_ID,
								ContextStructure* c_struct,
								std::ostream& out )
{
	out<< "Q_DB: \n";

	QDBKey key( c_struct );
	QDBValue value( c_struct );

	DbTxn* txn = NULL;
	Dbc* cursorp = NULL;
	Dbt dbtKey(NULL,0), dbtValue(NULL,0);

	std::string temp_str;
	Vdt temp_vdt;
	void* metadata;

	int counter = 0;
	const int num_per_line = 10;

	GetDBCursor( cursorp, txn, partition_ID );

	out<< "DzName,Key,DirtyOrClean,Q_value,Size,UpdateTimestamp,Metadata\n";

	while( cursorp->get( &dbtKey, &dbtValue, DB_NEXT ) == 0 )
	{
		key.setValues( dbtKey.get_data(), dbtKey.get_size() );
		value.setValues( dbtValue.get_data(), dbtValue.get_size() );

		//out<< '\"';
		temp_vdt = key.getDzName();
		temp_str = "";
		temp_str.insert( 0, temp_vdt.get_data(), temp_vdt.get_size() );
		out<< temp_str << ",";
	
		temp_vdt = key.getKey();
		out<< *(int*) temp_vdt.get_data() << ",";

		//temp_str = "";
		//temp_str.insert( 0, temp_vdt.get_data(), temp_vdt.get_size() );
		//out<< temp_str << ",";

		out<< key.getHeaderFlag() << ",";
		out<< key.getQValue() << ",";

		out<< value.getRecordSize() << ",";
		out<< value.getUpdateTimestamp().QuadPart << ",";
		
		out<< "Metadata: ";
		metadata = value.getMetadataPtr();

		cachingAlgorithm->printMetadata( metadata, out );

		out<< '\n';
	}
	out<< "\n";

	cursorp->close();
	txn->commit( 0 );

}
