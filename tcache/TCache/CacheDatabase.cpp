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
#include "CacheDatabase.h"


//CacheKey Class
CacheKey::CacheKey(ContextStructure *context_struct)
{
	m_context_struct = context_struct;
}

CacheKey::~CacheKey()
{
}

void CacheKey::setValues( const Vdt& DZName, const Vdt& Key)
{
	m_dzName = DZName.get_data();
	m_dzNameSize = (uchar)DZName.get_size();
	m_separator = TC_CONCAT_SEP;
	m_key = Key.get_data();
	m_keySize = (uchar)Key.get_size();
}

void CacheKey::setValues(const char& headerFlag, const Vdt& DZName, const Vdt& Key)
{
	m_header = headerFlag;
	setValues( DZName, Key );
	
	/*m_dzName = DZName.get_data();
	m_dzNameSize = (uchar)DZName.get_size();
	m_separator = TC_CONCAT_SEP;
	m_key = Key.get_data();
	m_keySize = (uchar)Key.get_size();*/
}

void CacheKey::setValues( void* data, const int& data_size  )
{
	char* data_p = (char*)data;
	int datalen = 0;

	datalen = sizeof(m_header);
	m_header = *(char*)data_p;
	data_p += datalen;

	Vdt dzkey( (char*)data_p, data_size - datalen);
	Vdt dzname;
	Vdt key;

	TCUtility::splitKeyDatazonePair(dzkey, dzname, key);

	m_dzName = (char*)dzname.get_data();
	m_dzNameSize = dzname.get_size();

	m_key = (char*)key.get_data();
	m_keySize = key.get_size();

}

void CacheKey::getData(char* &data, int &size)
{
	int datalen = 0;
	size = 0;
	char* data_p;

	size += sizeof( m_header );
	size += m_dzNameSize;
	size += sizeof( m_separator );
	//size += sizeof( m_keySize );
	size += m_keySize;

	// Resize buffer if it is too small
	if( size > m_context_struct->cachedb_key_size )
	{
		ContextManager::ResizeMemory(	m_context_struct->cachedb_key,
										size );
		m_context_struct->cachedb_key_size = size;
	}

	data_p = m_context_struct->cachedb_key;

	// Return pointer to the buffer location
	data = m_context_struct->cachedb_key;	

	datalen = sizeof( m_header );
	memcpy( data_p, &m_header, datalen );
	data_p += datalen;

	datalen = m_dzNameSize ;
	memcpy( data_p, m_dzName, datalen );
	data_p += datalen;

	datalen = sizeof( m_separator );
	memcpy( data_p, &m_separator, datalen );
	data_p += datalen;

	datalen = m_keySize ;
	memcpy( data_p, m_key, datalen );
	data_p += datalen;

}

//CacheValue Class
CacheValue::CacheValue(ContextStructure *context_struct)
{
	m_context_struct = context_struct;
}

CacheValue::~CacheValue()
{
}

void CacheValue::setDataRecValues( const Vdt& Value )
{
	m_dataBuf = (void*)Value.get_data();
	m_dataBuf_size = Value.get_size();
}

void CacheValue::setDataRecValues( void *data, const int &data_size)
{
	m_dataBuf = data;
	m_dataBuf_size = data_size;
}

void CacheValue::setMDValues( void *data, const int &data_size)
{
	char* data_p = (char*)data;;

	m_dirtyBit = *(char*)data_p;
	data_p += sizeof(m_dirtyBit);
	m_QValue = *(TCQValue*)data_p;
	data_p += sizeof(m_QValue);

}

void CacheValue::setMDValues(const char& dirtybit, const TCQValue& QValue )
{
	m_dirtyBit = dirtybit;
	m_QValue =  QValue;
}

void CacheValue::getDataRec( void* &data, int &size)
{
	data = m_dataBuf;
	size = m_dataBuf_size;
}

void CacheValue::getMD( void* &data, int &size)
{
	int datalen = 0;
	size = 0;
	char* data_p;

	size += sizeof( m_dirtyBit );
	size += sizeof( m_QValue );

	// Resize buffer if it is too small
	if( size > m_context_struct->md_lookup_value_size )
	{
		// TODO: It seems as though we need extra space, maybe due to padding?
		ContextManager::ResizeMemory(	m_context_struct->md_lookup_value,
										size );
		m_context_struct->md_lookup_value_size = size;
	}

	data_p = m_context_struct->md_lookup_value;

	// Return pointer to the buffer location
	data = m_context_struct->md_lookup_value;	

	datalen = sizeof( m_dirtyBit );
	memcpy( data_p, &m_dirtyBit, datalen );
	data_p += datalen;

	datalen = sizeof( m_QValue );
	memcpy( data_p, &m_QValue, datalen );
	data_p += datalen;
}

//CacheDatabase Class

CacheDatabase::CacheDatabase(void)
{
}

CacheDatabase::~CacheDatabase(void)
{
}



/*
*	CacheDatabase Class
*/

int CacheDatabase::Initialize( DbEnv* memoryDbEnv, const ConfigParametersV& cp )
{
	int retVal = -1;
	string temp_dbname;
	Db* dbp;
	DbMpoolFile *mpf = NULL;

	m_numberofpartitions = cp.MemBDB.DatabaseConfigs.Partitions;
	m_bdbMemEnv = memoryDbEnv;
	
	//Set up Data_rec Database Array
	m_bdbCacheArray = new Db*[m_numberofpartitions];

	for( int i = 0; i < m_numberofpartitions; i++ )
	{
		temp_dbname = TC_MEMORY_DBNAME;
		temp_dbname += TCUtility::intToString( i );

		dbp = new Db( m_bdbMemEnv, 0 );

		if( cp.MemBDB.DatabaseConfigs.CacheDBPageSize > 0 )
			dbp->set_pagesize( cp.MemBDB.DatabaseConfigs.CacheDBPageSize );
		
		retVal = dbp->open(	NULL,
							NULL,
							temp_dbname.c_str(),
							//DB_HASH,
							DB_BTREE,
							DB_CREATE | DB_AUTO_COMMIT,
							0 );

		if(retVal != 0)
			break;

		mpf = dbp->get_mpf();
		retVal = mpf->set_flags( DB_MPOOL_NOFILE, 1 );
		if( retVal != 0 )
			std::cout<< "QDatabase: Error. Could not set DbMpoolFile\n";

		m_bdbCacheArray[i] = dbp;
	}

	return retVal;
}

int CacheDatabase::Shutdown()
{
	int retVal = -1;
	//Close Data_rec Array
	for( int i = 0; i < m_numberofpartitions ; i++ )
	{
		retVal = m_bdbCacheArray[i]->close( 0 );
		if( retVal != 0 )
		{
			// TODO: handle error closing database
		}

		delete m_bdbCacheArray[i];
	}

	delete [] m_bdbCacheArray;
	m_bdbCacheArray = NULL;

	return retVal;
}

//Get BDB Db* handler for the corresponfding partition 
int	CacheDatabase::getDbp( const int& partition_ID, Db* &ret_dbp )
{
	if( partition_ID < 0 || partition_ID >= m_numberofpartitions )
		return -1;

	ret_dbp = m_bdbCacheArray[partition_ID];
	return 0;
}

//Insert Data record in Cache
int CacheDatabase::InsertDataRec( CacheKey &Key,  CacheValue &Value, const int partition_ID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. couldn't get Db handle for Data_rec Insert in CacheDB" << std::endl;
		return -1;
	}

	//Set header flag for Data_rec
	Key.setDataRecHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );
	
	
	void* dataBuf;
	int dataBufSize = 0;

	Value.getDataRec(dataBuf, dataBufSize);
	// Convert value to a Dbt
	Dbt dbtValue( dataBuf, dataBufSize );
	dbtValue.set_ulen( dataBufSize );
	dbtValue.set_flags( DB_DBT_USERMEM );



	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->put(NULL, &dbtKey, &dbtValue, 0);
			//if( retVal == 0 )
				tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//numTries++;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				if( retVal == BDB_FAILED_MEM_FULL )
				{
					retVal = TC_FAILED_MEM_FULL;
					//cout<< ex.what() << endl;
				}
				else
					std::cout<< "Unknown Error. Object couldn't be inserted in Datarec Cache  - retVal: " << retVal << std::endl;
				
				tryagain = false;
			}
		}
	}

	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. Object couldn't be inserted in Datarec " << std::endl;

	return retVal;
}


//Insert Metadata Lookup in Cache
int CacheDatabase::InsertMDLookup( CacheKey &Key,  CacheValue &Value, const int partition_ID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( CacheDatabase::getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. Couldn't get Db handle for MD Insert in CacheDB" << std::endl;
		return -1;
	}

	//Set header flag for MD
	Key.setMDHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );
	
	
	void* dataBuf;
	int dataBufSize = 0;

	Value.getMD(dataBuf, dataBufSize);
	// Convert value to a Dbt
	Dbt dbtValue( dataBuf, dataBufSize );
	dbtValue.set_ulen( dataBufSize );
	dbtValue.set_flags( DB_DBT_USERMEM );

	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->put(NULL, &dbtKey, &dbtValue, 0);
			//if( retVal == 0 )
				tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//numTries++;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				if( retVal == BDB_FAILED_MEM_FULL )
				{
					retVal = TC_FAILED_MEM_FULL;
					//cout<< ex.what() << endl;
				}
				else
					std::cout<< "Unknown Error. Object couldn't be inserted in MD Cache  - retVal: " << retVal << std::endl;
				
				tryagain = false;
			}
		}
	}

	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. Object couldn't be inserted in MD Cache " << std::endl;

	return retVal;

}


//Get Data record from Cache
int CacheDatabase::GetDataRec(  CacheKey& Key,  CacheValue& Value, const int partition_ID, stats* stats_update )
{
	int retVal = -1;

	void* dataBuf;
	int dataBufSize = 0;

	Value.getDataRec(dataBuf, dataBufSize);
	// Convert value to a Dbt
	Dbt dbtValue( dataBuf, dataBufSize );
	dbtValue.set_ulen( dataBufSize );
	dbtValue.set_flags( DB_DBT_USERMEM );

	retVal = GetDataRec( Key, &dbtValue, partition_ID, stats_update );

	// Resizes the value buffer if it is too small
	//if( retVal == DB_BUFFER_SMALL )
	Value.setDataRecSize( dbtValue.get_size() );

	return retVal;
}


//Get Data record from Cache
int CacheDatabase::GetDataRec(  CacheKey& Key,  Dbt* dbtValue, const int partition_ID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( CacheDatabase::getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. Couldn't get Db handle for Data_rec Get in CacheDB" << std::endl;
		return -1;
	}

	//Set header flag for Data_rec
	Key.setDataRecHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );

	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->get(NULL, &dbtKey, dbtValue, 0);
			//if( retVal == 0 )
				tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//std::cout<< ex.what() << std::endl;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				tryagain = false;
			}
		}
	}

	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. Data_rec Object coulnd't be retrieved from Cache " << std::endl;

	return retVal;
}



//Get Metadata from Cache
int CacheDatabase::GetMDLookup(  CacheKey& Key,  CacheValue& Value, const int partition_ID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( CacheDatabase::getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. couldn't get Db handle for MD Get in CacheDB" << std::endl;
		return -1;
	}

	//Set header flag for MD
	Key.setMDHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );
	
	// Get data block from ContextManager struct
	ContextStructure* cstruct = Value.getContextStruct();
	Dbt dbtValue(cstruct->md_lookup_value, cstruct->md_lookup_value_size);
	dbtValue.set_ulen( cstruct->md_lookup_value_size );
	dbtValue.set_flags( DB_DBT_USERMEM );

	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->get(NULL, &dbtKey, &dbtValue, 0);
			//if( retVal == 0 )
				tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//std::cout<< ex.what() << std::endl;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else if( retVal == DB_BUFFER_SMALL )
			{
				if( dbtValue.get_size() > (uint)cstruct->md_lookup_value_size )
				{
					ContextManager::ResizeMemory( cstruct->md_lookup_value, dbtValue.get_size() );
					cstruct->md_lookup_value_size = dbtValue.get_size();

					dbtValue.set_data( cstruct->md_lookup_value );
					dbtValue.set_ulen( cstruct->md_lookup_value_size );
				}
				else if( dbtKey.get_size() > (uint)cstruct->md_lookup_key_size )
				{
					ContextManager::ResizeMemory( cstruct->md_lookup_key, cstruct->md_lookup_key_size );
					cstruct->md_lookup_key_size = dbtKey.get_size();

					dbtKey.set_data( cstruct->md_lookup_key );
					dbtKey.set_ulen( cstruct->md_lookup_key_size );
				}
			}
			else
				tryagain = false;
		}
	}

	void* data_p = dbtValue.get_data();
	int dataSize_p = dbtValue.get_size();

	if( retVal == 0 )
		Value.setMDValues(data_p, dataSize_p);
	
	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. MD Object coulnd't be retrieved from Cache " << std::endl;

	return retVal;
}

//Delete Data record from Cache
int CacheDatabase::DeleteDataRec(  CacheKey& Key, const int partition_ID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( CacheDatabase::getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. couldn't get Db handle for Data_rec Delete in CacheDB" << std::endl;
		return -1;
	}

	//Set header flag for Data_rec
	Key.setDataRecHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );

	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->del(NULL, &dbtKey, 0 );
			//if( retVal == 0 )
			tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//std::cout<< ex.what() << std::endl;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				//std::cout<< ex.what() << std::endl;
				//std::cout<< retVal << std::endl;
				tryagain = false;
			}
		}
	}

	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. Data_rec Object coulnd't be deleted from Cache " << std::endl;

	return retVal;
}
	

//Delete Metadata from Cache
int CacheDatabase::DeleteMDLookup(  CacheKey& Key, const int partition_ID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( CacheDatabase::getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. couldn't get Db handle for MD Delete in CacheDB" << std::endl;
		return -1;
	}

	//Set header flag for MD
	Key.setMDHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );

	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->del(NULL, &dbtKey, 0 );
			//if( retVal == 0 )
				tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//std::cout<< ex.what() << std::endl;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				//std::cout<< ex.what() << std::endl;
				//std::cout<< retVal << std::endl;
				tryagain = false;
			}
		}
	}

	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. MD Object coulnd't be deleted from Cache " << std::endl;

	return retVal;
}

bool CacheDatabase::Exists( CacheKey &Key, const int partitionID, stats* stats_update )
{
	int retVal = -1;
	int numTries = 0;
	bool tryagain = true;

	// Retrieve Db* for relevant partition
	Db* dbp = NULL;
	if( CacheDatabase::getDbp( partitionID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
		std::cout<< "Error. Couldn't get Db handle for Data_rec Get in CacheDB" << std::endl;
		return false;
	}

	//Set header flag for Data_rec
	Key.setDataRecHeader();

	char* keyBuf;
	int keyBufSize = 0;

	Key.getData(keyBuf, keyBufSize);
	// Convert value to a Dbt
	Dbt dbtKey( keyBuf, keyBufSize );
	dbtKey.set_ulen( keyBufSize );
	dbtKey.set_flags( DB_DBT_USERMEM );

	while( tryagain && numTries < TC_DEADLOCK_NUMBER_OF_TRIES )
	{
		numTries++;

		try
		{
			retVal = dbp->exists(NULL,&dbtKey,0); 
			tryagain = false;
		}
		catch(DbException ex)
		{
			retVal = ex.get_errno();
			if( retVal == DB_LOCK_DEADLOCK )
			{
				//std::cout<< ex.what() << std::endl;
				if( stats_update )
					stats_update->NumDeadlocks++;
			}
			else
			{
				tryagain = false;
			}
		}
	}

	if( retVal != 0 && numTries >= TC_DEADLOCK_NUMBER_OF_TRIES )
		std::cout<< "Error. Data_rec Object coulnd't be retrieved from Cache " << std::endl;

	if(retVal==0)
		return true;
	else 
		return false;
}

int CacheDatabase::GetDBCursor( Dbc* &cursorp, DbTxn* &txn, const int& partition_ID, stats* stats_update )
{
	// Retrieve Db* for specific partition
	Db* dbp = NULL;
	if( getDbp( partition_ID, dbp ) != 0 )
	{
		// TODO: handle error, couldn't get open database handle
	}

	int ret;
	m_bdbMemEnv->txn_begin( NULL, &txn, 0 );

	try
	{
		ret = dbp->cursor( txn, &cursorp, 0 );
	}
	catch( DbException& e )
	{
		// TODO: handle error get cursor fail
		std::cout<< "CacheDatabase::GetCursor: " << e.what();
		ret = -1;
		txn->abort();
	}

	return ret;
}


void CacheDatabase::printContents(	const int& partition_ID,
								ContextStructure* c_struct,
								std::ostream& out )
{
	out<< "CacheDatabase: \n";
 
	stats stats_update;

	CacheKey key( c_struct );
	CacheValue value( c_struct );

	DbTxn* txn = NULL;
	Dbc* cursorp = NULL;
	Dbt dbtKey(NULL,0), dbtValue(NULL,0);

	std::string temp_str;
	Vdt temp_vdt;

	GetDBCursor( cursorp, txn, partition_ID, &stats_update );

	while( cursorp->get( &dbtKey, &dbtValue, DB_NEXT ) == 0 )
	{
		key.setValues( dbtKey.get_data(), dbtKey.get_size() );
		//value.setValues( dbtValue.get_data(), dbtValue.get_size() );

		out<< key.getHeaderFlag() << ": ";

		temp_vdt = key.getDzName();
		temp_str = "";
		temp_str.insert( 0, temp_vdt.get_data(), temp_vdt.get_size() );

		out<< temp_str << ": ";
	
		temp_vdt = key.getKey();
		out<< *(int*) temp_vdt.get_data() << ": " << endl;
	
		//out<< "\n";
	}
	out<< "\n";

	cursorp->close();
	txn->commit( 0 );

}
