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
#include "DatazoneManager.h"
#include "TCache.h"

//#ifdef EXTRA_DEBUG
#define HASH_INDEX_PARTITIONID_SEPERATOR "__"

int DatazoneManager::OpenHandle(const char *dzname,int length,Db* &dbptr)
{
	Db *dz_dbp = NULL;

	try {
		dz_dbp = new Db(m_diskDBEnv, 0);

		int open_flags = DB_THREAD;          // Allow autocommit

		int ret = dz_dbp->open(NULL,		// Txn pointer
        		  dzname,   // File name
                  //datazone_name.c_str(),   
				  NULL,       // Logical db name
                  DB_HASH,   // Database type (using hash)
                  open_flags,  // Open flags
                  0);         // File mode. Using defaults

		dbptr=dz_dbp;	
				    
	} catch(DbException &e) {
        printf("%s\n",e.what()); 
		if (dz_dbp!=NULL)
			dz_dbp->close(0);
		return (-1);
    } catch(exception &e) {
        printf("%s\n",e.what()); 
        return (-1);
    }
	return 0;
}

int DatazoneManager::OpenHandleNoEnv(const char *dznameFilePath,int length, Db* &dbptr)
{
	Db *dz_dbp = NULL;
	try {
		dz_dbp = new Db(NULL, 0);

		int open_flags = DB_THREAD;          // Allow autocommit

		int ret = dz_dbp->open(NULL,		// Txn pointer
                  dznameFilePath,   // File name
                  //datazone_name.c_str(),   
				  NULL,       // Logical db name
                  DB_HASH,   // Database type (using hash)
                  open_flags,  // Open flags
                  0);         // File mode. Using defaults

		dbptr=dz_dbp;	
				    
	} catch(DbException &e) {
        printf("%s\n",e.what()); 
		if (dz_dbp!=NULL)
			dz_dbp->close(0);
		return (-1);
    } catch(exception &e) {
        printf("%s\n",e.what()); 
        return (-1);
    }
	return 0;
}


short DatazoneManager::GetNumPartition(const Vdt &dzname)
{
	string s_pid_dzname="";
	s_pid_dzname.insert( 0, HASH_INDEX_PARTITIONID_SEPERATOR, strlen(HASH_INDEX_PARTITIONID_SEPERATOR));
	s_pid_dzname.insert(strlen(HASH_INDEX_PARTITIONID_SEPERATOR),dzname.get_data(),dzname.get_size());
	const char* pid_dzname=s_pid_dzname.c_str();
	
	int ret;
	Db *DbHandle=NULL;
	short hash_index_num_partitions=-1;
	DbHandle = m_HI->Get(pid_dzname, dzname.get_size()+strlen(HASH_INDEX_PARTITIONID_SEPERATOR),hash_index_num_partitions);
	try {
		if(DbHandle==NULL && hash_index_num_partitions==-1)
		{
			
			Dbt key(NULL,0), value(NULL,0);

			key.set_data( dzname.get_data() );
			key.set_size( dzname.get_size() );
			key.set_ulen (dzname.get_size());
			key.set_flags(DB_DBT_USERMEM);
			
			value.set_data(&hash_index_num_partitions);
			value.set_size(sizeof(short));
			value.set_ulen(sizeof(short));
			value.set_flags(DB_DBT_USERMEM);

			ret=m_catalog->get( NULL, &key, &value, 0 );
			if(ret)
			{
				printf("DatazoneManager:Error reading number of partitions for %s datazone from catalog \n",dzname.get_data());
				return(ret);
			}
			
			int ret=m_HI->Put(pid_dzname, dzname.get_size()+strlen(HASH_INDEX_PARTITIONID_SEPERATOR), NULL,hash_index_num_partitions);
			
			if(ret!=0)
			{
#ifdef EXTRA_DEBUG
				printf("\nDatazoneManager: Error insering into hash table\n");
#endif
				
			}
		}
		else{
#ifdef EXTRA_DEBUG
			printf("Number of partitions from hash table%d\n",hash_index_num_partitions); 
#endif
		}
	}
	catch(DbException &e) {
		printf("%s\n",e.what()); 
		return (e.get_errno());
	} catch(exception &e) {
		printf("%s\n",e.what()); 
		return (-1);
	}   
	return hash_index_num_partitions;
}



int DatazoneManager::CloseHandle(const Vdt &dzname,const int PartitionID)
{
	int ret=0;
	string s_dzname="";
	Db *DbHandle;
	
	AppendPartitionID(dzname,PartitionID,s_dzname);
	
	short hash_index_pid;
	DbHandle= m_HI->Get(s_dzname.c_str(),strlen(s_dzname.c_str())+1,hash_index_pid);
	if(DbHandle==NULL)
	{
		return -1;
	}
	else {
		DbHandle= m_HI->Get(s_dzname.c_str(),strlen(s_dzname.c_str())+1,hash_index_pid);
		if(DbHandle==NULL)
			return -1;
		ret=m_HI->Delete(s_dzname.c_str(),strlen(s_dzname.c_str())+1);
		if(DbHandle!=NULL && ret==0)
		{
			ret=DbHandle->close(0);
			delete DbHandle;
		}
	}
	return ret;
}

int DatazoneManager::CloseHandle( const Vdt &dzName, const Vdt &Key)
{
	int partitionID=GetPartitionID(dzName,Key);
	int ret=CloseHandle(dzName,partitionID);
	return ret;
}

bool DatazoneManager::checkIfHandleExistsInHashIndex(const Vdt&dzname, const Vdt &Key)
{
	int partitionID=GetPartitionID(dzname,Key);
	string s_dzname="";
	Db *DbHandle;
	AppendPartitionID(dzname,partitionID,s_dzname);
	short hash_index_pid;
	DbHandle= m_HI->Get(s_dzname.c_str(),strlen(s_dzname.c_str())+1,hash_index_pid);
	if(DbHandle==NULL)
		return false;
	else
		return true;
}
int DatazoneManager::Initialize( DbEnv* diskEnvironment, const ConfigParametersV& cp  )
{
	int ret=0;
	m_numOfPartitions = cp.DiskBDB.DatabaseConfigs.Partitions;
	m_dbPageSize = cp.DiskBDB.DatabaseConfigs.PageSize;

	m_diskDBEnv = diskEnvironment;

	m_catalog = new Db( m_diskDBEnv, 0 );

	int open_flags = DB_CREATE|DB_THREAD;          // Allow autocommit

	ret = m_catalog->open(	NULL,
							DM_CATALOG,
							NULL,
							DB_HASH,
							open_flags,
							0 );

	m_dzNameBufferSize = TC_LARGE_BUFFER_SIZE;

	m_HI=new HashIdx();

	return ret;
}

int DatazoneManager::Shutdown()
{
	Db *handle;
	int ret;
	do
	{
		ret=m_HI->DeleteOneKeyAndReturnDB(handle);
		if(!ret)
		{
			if(handle!=NULL)
			{
				handle->close(0);
				delete handle;
			}
		}
	}
	while(ret==0);
	m_catalog->close(0);
	delete m_catalog;

	return 0;
}

int DatazoneManager::Exists( const Vdt &dzName )
{
	Dbt key(NULL,0);
	key.set_data( dzName.get_data() );
	key.set_size( dzName.get_size() );
	return m_catalog->exists( NULL, &key, 0 );
}

int DatazoneManager::AppendPartitionID(const Vdt&dzName, const int PartitionID,string &dzname)
{

	int length=dzName.get_size();
	if( (char) *(dzName.get_data()+ (length-1))=='\0')
	{
		length--;
	}
	
	dzname.insert( 0, (char*)dzName.get_data(), length);
	dzname+=TCUtility::intToString(PartitionID);

	return 0;
}

// Called by Create(Datazone)
int DatazoneManager::Create( const Vdt &dzName )
{
	// Check if the dzName exist 
	int ret;

	ret=Exists(dzName );
	if(ret==0)
	{
		return -1; // Datazone exists
	}
	
	Db* dbp; 

	// Create "numofPartitions" files for the dzname 
	for(int i=0;i<m_numOfPartitions;i++)
	{
		dbp= new Db( m_diskDBEnv, 0 );
		string s_dzname="";
		AppendPartitionID(dzName,i,s_dzname);
		int open_flags = DB_CREATE;          // Allow autocommit
		//cout<<filename<<endl;

		if( m_dbPageSize > 0 )
			dbp->set_pagesize( m_dbPageSize );

		ret = dbp->open(	NULL, 
							//(dzname+intToString(i)).c_str(),
							s_dzname.c_str(),
							NULL,
							DB_HASH,		
							open_flags,
							0 );
		if( ret != 0 )
			return ret;

		dbp->close(0);
		delete dbp;		
	}
	
	// Insert the dzname and the num of partitions in the catalog
	Dbt key(NULL,0), value(NULL,0);

	key.set_data( dzName.get_data() );
	key.set_size( dzName.get_size() );

	value.set_data(&m_numOfPartitions);
	value.set_size(sizeof(short));

	return m_catalog->put( NULL, &key, &value, 0 );
}

int DatazoneManager::Get( const Vdt &dzName, const Vdt &Key, Db* &retDBptr )
{
	
	int partitionID=GetPartitionID(dzName,Key);
	string s_dzname="";
	AppendPartitionID(dzName,partitionID,s_dzname);
	int ret=GetHandleForPartition( s_dzname.c_str(), strlen(s_dzname.c_str())+1, retDBptr );
	return ret;
}

int DatazoneManager::GetHandleForPartition(const char *dzname,const int length, Db* &Dbptr )
{

	int ret;
	Db *DbHandle=NULL;
	short hash_index_pid=-1;
	DbHandle = m_HI->Get(dzname, length,hash_index_pid);
	try {
		if(DbHandle==NULL)
		{
			//ret=Open(dzName.get_data(),DbHandle);
			ret=OpenHandle(dzname,length,DbHandle);

			if(ret)
			{
				printf("DatazoneManager:Error opening %s database or database does not exist\n",dzname);
				return(ret);
			}

			int ret=m_HI->Put(dzname, length, DbHandle,hash_index_pid);
			if(ret!=0) 
			{
				if(DbHandle !=NULL)
				{
					DbHandle->close(0);
					delete DbHandle;
				}


				Db *Dbp = m_HI->Get(dzname, length,hash_index_pid);
				Dbptr=Dbp;
			}
			else
				Dbptr=DbHandle;
		}
		else{
			Dbptr=DbHandle;
		}
	}
	catch(DbException &e) {
		printf("%s\n",e.what()); 
		return (e.get_errno());
	} catch(exception &e) {
		printf("%s\n",e.what()); 
		return (-1);
	}   
	return 0;

}

inline int DatazoneManager::GetPartitionID(const Vdt& dzname, const Vdt&key)
{
	int numOfPartitions=GetNumPartition(dzname);
	return TCUtility::bernsteinHash(dzname,key,numOfPartitions);
}

int DatazoneManager::GetDBCursor( Dbc* &cursorp ) const
{
	return m_catalog->cursor( NULL, &cursorp, 0 );	
}

int DatazoneManager::getNumDatazones()
{
	DB_HASH_STAT* stats = 0;
	m_catalog->stat( NULL, &stats, 0 );
	int numKeys = stats->hash_nkeys;
	delete stats;
	return numKeys;
}

int DatazoneManager::GetAllDatazones(Vdt *dzNameArr, int* NumberOfDataZones)
{
	int ret = 0;

	int num_dz = getNumDatazones();
	if( num_dz != *NumberOfDataZones )
	{
		if( num_dz > *NumberOfDataZones )
		{
			*NumberOfDataZones = num_dz;
			return TC_BAD_ARRAYSIZE;
		}
		else
		{
			// Set the correct value and continue since there is enough space
			*NumberOfDataZones = num_dz;
		}
	}

	Dbc* cursorp;
	ret = GetDBCursor( cursorp );
	if( ret != 0 )
		return ret;

	Dbt key(NULL,0), value(NULL,0);
	int counter = 0;

	// Reset buffer used to store datazone names for the Vdt to point to
	memset( m_dzNameBuffer, 0, m_dzNameBufferSize );
	m_dzNameBufferPos = 0;

	// TODO: make sure the memory can be accessed properly by calling function
	//		 does the Vdt data ptr still have access after awm_HIle?
	// answer: it does not. need buffer: m_dzNameBuffer
	while( cursorp->get( &key, &value, DB_NEXT ) == 0 && counter < *NumberOfDataZones )
	{
		memcpy( m_dzNameBuffer + m_dzNameBufferPos, key.get_data(), key.get_size() );
		
		dzNameArr[counter].set_data( m_dzNameBuffer + m_dzNameBufferPos );
		dzNameArr[counter].set_size( key.get_size() );
		
		counter++;
		m_dzNameBufferPos += key.get_size();
	}

	cursorp->close();

	return ret;
}

void DatazoneManager::_deleteAll()
{
	Dbc* cursorp;
	int ret;
	GetDBCursor( cursorp );

	Dbt key(NULL, 0), value(NULL, 0);
	string filename;

	// Delete entries from the hash index
	ret=Shutdown();

	while( cursorp->get( &key, &value, DB_NEXT ) == 0 )
	{
		filename = "";
		filename.insert( 0, (char*)key.get_data(), key.get_size() );

		ret = m_diskDBEnv->dbremove( NULL, filename.c_str(), NULL, 0 );
		if( ret != 0 )
			cout<< "DatazoneManager::_deleteAll : Error, could not delete database file.\n";

		cursorp->del( 0 );
	}

	cursorp->close();	
}


int DatazoneManager::RenameDZFromCataloge( const Vdt& dzName, const int renameFlag )
{
	int ret = -1;
	string s_dzname="";
	string s_newdzname="";
	Vdt newdzname;

	/*
	*	Rename Datazone entry from Cataloge 
	*/

	Dbt key;
	Dbt newKey;
	Dbt value;
	//char buffer_key[TC_MAX_CATALOGE_VALUE_SIZE];
	char buffer_value[TC_MAX_CATALOGE_VALUE_SIZE];

	value.set_data( &buffer_value );
	value.set_size( sizeof(int) );
	value.set_ulen( sizeof(int) );
	value.set_flags( DB_DBT_USERMEM );

	key.set_data( dzName.get_data() );
	key.set_size( dzName.get_size() );

	//create new dzname
	s_newdzname.insert( 0, dzName.get_data(), dzName.get_size() );
	//s_newdzname += "_trunc";
	s_newdzname += TC_TRUCATE_DZONE_MASK;
	newKey.set_data( (char*)s_newdzname.c_str() );
	newKey.set_size( s_newdzname.length() );
	
	try
	{
		if(renameFlag == TC_RENAME_DZ_ORIGINAL)
		{
			//get datazone entry from cataloge
			ret = m_catalog->get( NULL, &newKey, &value, 0 );
			if( ret != 0 )
			{
				cout<< "DatazoneManager::RenameDZFromCataloge : DZone_renamed not found in Cataloge : " << s_dzname << ".\n";
				return ret;
			}
			//insert new datazone name with value = num_partitions
			ret = m_catalog->put( NULL, &key, &value, 0 );
			if( ret != 0 )
			{
				cout<< "DatazoneManager::RenameDZFromCataloge : Failed to insert DZone in Cataloge : " << s_dzname << ".\n";
				return ret;
			}

			//delete datazone entry from Cataloge
			ret = m_catalog->del( NULL, &newKey, 0 );
			if( ret != 0 )
			{
				cout<< "DatazoneManager::RenameDZFromCataloge : Failed to delete DZone_renamed from Cataloge : " << s_dzname << ".\n";
				return ret;
			}
		}
		else if(renameFlag == TC_RENAME_DZ_TRUNCATE)
		{
			//get datazone entry from cataloge
			ret = m_catalog->get( NULL, &key, &value, 0 );
			if( ret != 0 )
			{
				cout<< "DatazoneManager::RenameDZFromCataloge : DZone not found in Cataloge : " << s_dzname << ".\n";
				return ret;
			}
			//insert new datazone name with value = num_partitions
			ret = m_catalog->put( NULL, &newKey, &value, 0 );
			if( ret != 0 )
			{
				cout<< "DatazoneManager::RenameDZFromCataloge : Failed to insert DZone_renamed in Cataloge : " << s_dzname << ".\n";
				return ret;
			}
			//delete datazone entry from Cataloge
			ret = m_catalog->del( NULL, &key, 0 );
			if( ret != 0 )
			{
				cout<< "DatazoneManager::RenameDZFromCataloge : Failed to delete DZone from Cataloge : " << s_dzname << ".\n";
				return ret;
			}
		}
	}
	catch(DbException ex)
	{
		ret = ex.get_errno();
		cout<< ex.what() << endl;
		if( ret == DB_LOCK_DEADLOCK )
		{
			//handle deadlock, try again??
		}
		//return ret;
	}

	return ret;
}


int DatazoneManager::Rename( const Vdt& dzName, const int renameFlag )
{
	int ret = -1;
	Db* retDBptr;
	string s_dzname="";
	string s_newdzname="";
	Vdt newdzname;
	//should Number of partitions from cataloge
	int numPartition = 0;
	numPartition = GetNumPartition( dzName );
	Db *dbptr = new Db(NULL, 0);
	const char **pathArray;
	m_diskDBEnv->get_data_dirs(&pathArray);
	string dataDir_init = pathArray[0];
	string dataDir, s_dirFile, s_newDirFile;

	TCUtility::fixDataDirectoryPath( dataDir_init, dataDir );
		
	ret = RenameDZFromCataloge( dzName, renameFlag );
	if( ret != 0 ) 
	{
		cout<< "DatazoneManager::Rename : Failed to rename Cataloge entry: " << s_dzname << ".\n";
		return ret;
	}
	
	for(int i = 0; i < numPartition; i++)
	{
		s_dirFile.clear();
		s_dzname.clear();
		s_newdzname.clear();
		AppendPartitionID(dzName, i , s_dzname);

		//create new dzname
		s_newdzname.insert( 0, s_dzname.c_str(), s_dzname.length() );
		s_newdzname += TC_TRUCATE_DZONE_MASK;

		//change name to original (after truncate process)
		if(renameFlag == TC_RENAME_DZ_ORIGINAL)
		{
			newdzname.set_data((char*)s_newdzname.c_str());
			newdzname.set_size(s_newdzname.length());
			//close Db datazone partition handle
			ret = CloseHandle( newdzname, i );
			if( ret != 0)
				cout<< "DatazoneManager::Rename : DZone Partition not found in Hashtable : " << s_dzname << ".\n";

			//rename file Db datazone partition
			s_dirFile = dataDir + s_dzname;
			s_newDirFile = dataDir + s_newdzname;
			ret = dbptr->rename( s_newDirFile.c_str(), NULL, s_dirFile.c_str(), 0 );
			//ret = m_diskDBEnv->dbrename( NULL, s_newdzname.c_str(), NULL, s_dzname.c_str(), DB_AUTO_COMMIT);
			if( ret != 0)
			{
				cout<< "DatazoneManager::Rename : Error, could not rename database file: " << s_dzname << ".\n";
				return ret;
			}
		}
		//change name to include mask at the end, (before truncate process)
		else if(renameFlag == TC_RENAME_DZ_TRUNCATE)
		{
			//close Db datazone partition handle
			ret = CloseHandle( dzName, i );
			if( ret != 0)
				cout<< "DatazoneManager::Rename : DZone Partition not found in Hashtable : " << s_dzname << ".\n";

			//ret = m_diskDBEnv->dbremove( NULL, s_dzname.c_str(), NULL, 0 );

			//rename file Db datazone partition
			s_dirFile = dataDir + s_dzname;
			s_newDirFile = dataDir + s_newdzname;
			ret = dbptr->rename( s_dirFile.c_str() , NULL, s_newDirFile.c_str(), 0 );
			//ret = m_diskDBEnv->dbrename(NULL, s_dzname.c_str(), NULL, s_newdzname.c_str(), DB_AUTO_COMMIT);
			if( ret != 0)
			{
				cout<< "DatazoneManager::Rename : Error, could not rename database file: " << s_dzname << ".\n";
				return ret;
			}
		}
	}
	
	return ret;
}