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
#include "DiskBDB.h"

int DiskBDB::Initialize(DbEnv* diskEnv,DatazoneManager *dzManager,const ConfigParametersV& cp)
{
	m_DiskEnv=diskEnv;
	m_dzManager=dzManager;
	m_maxDeadlockRetries = cp.DiskBDB.MaxDeadlockRetries;
	return 0;
}


int DiskBDB::Put (const Vdt& dzname, const Vdt& Key, const Vdt& Value, stats *stats_update)
{
	Db *DbHandle;
	int ret=0;
	ret=m_dzManager->Get(dzname,Key,DbHandle);
	
	if(ret!=0)
	{
		printf("Datazone handle not retrieved from datazone manager\n");
		return ret;
	}
	Dbt key(Key.get_data(),Key.get_size());
	Dbt value(Value.get_data(),Value.get_size()); 
	key.set_ulen(Key.get_size());
	key.set_flags( DB_DBT_USERMEM );
	value.set_ulen(Value.get_size() );
	value.set_flags( DB_DBT_USERMEM );
		
	bool keepTrying=true;
	int numTries=0;
	while(keepTrying && numTries < m_maxDeadlockRetries)
	{
		numTries++;
		try {

			ret=DbHandle->put(NULL,&key,&value,0);
			keepTrying=false;
		}
		catch(DbException &e) {
			//if(numTries==1)
				//printf("DiskBDB Put::%s\n",e.what()); 
			ret=e.get_errno();
			if(ret==DB_LOCK_DEADLOCK)
			{
				if(stats_update)
					stats_update->NumDeadlocks++;
			}
			else
				keepTrying=false;
			
				
		} catch(exception &e) {
			printf("%s\n",e.what()); 
			return (-1);
		}
	}
	return ret;
}

int DiskBDB::Delete (const Vdt&dzname, const Vdt&Key,stats *stats_update)
{
	Db *DbHandle;
	int ret=0;
	ret=m_dzManager->Get(dzname,Key,DbHandle);
	if(ret!=0)
	{
		cout<<"The Handle was not retrieved from datazone manager"<<endl;
		return ret;
	}

	Dbt key(Key.get_data(),Key.get_size());

	bool keepTrying=true;
	int numTries=0;
	while(keepTrying && numTries < m_maxDeadlockRetries)
	{
		numTries++;
		try {
			ret=DbHandle->del(NULL,&key,0);
			keepTrying=false;
		}
		catch(DbException &e) {
			if(numTries==1)
				printf("DiskBDB Delete::%s\n",e.what()); 
			
			ret=e.get_errno();
			if(ret==DB_LOCK_DEADLOCK)
			{
				if(stats_update)
					stats_update->NumDeadlocks++;
			}
			else
				keepTrying=false;
		} catch(exception &e) {
			cout << e.what() << endl;
			return (-1);
		}
	}
	return ret;
}

int DiskBDB::Truncate ( const Vdt *dzname )
{
	int ret = -1;
	int numPartition = 0;
	DbTxn* txn = NULL;
	Db *DbHandle;
	numPartition = m_dzManager->GetNumPartition( *dzname );
	string s_dzname = "";
	uint num_deleted = 0;
	const char **pathArray;
	m_DiskEnv->get_data_dirs(&pathArray);
	string s_datadir_init = pathArray[0];
	string s_datadir, s_pathFile;

	TCUtility::fixDataDirectoryPath( s_datadir_init, s_datadir );

	for( int i = 0; i < numPartition; i++ )
	{	
		//txn = NULL;
		//m_DiskEnv->txn_begin(NULL, &txn, 0);
		s_pathFile.clear();
		s_dzname.clear();
		//s_dzname = "";
		m_dzManager->AppendPartitionID(*dzname, i, s_dzname);
		s_dzname += TC_TRUCATE_DZONE_MASK;
		s_pathFile =  s_datadir + s_dzname;
		//ret = m_dzManager->GetHandleForPartition( s_dzname.c_str(), strlen(s_dzname.c_str()), DbHandle );
		//ret = m_dzManager->OpenHandleNoEnv( s_dzname.c_str(), strlen(s_dzname.c_str()), DbHandle );
		ret = m_dzManager->OpenHandleNoEnv( s_pathFile.c_str(), strlen(s_pathFile.c_str())+1, DbHandle );
		//handle ret!=0
		//blablabla...

		try
		{
			ret = DbHandle->truncate(NULL, &num_deleted, 0);
			//ret = DbHandle->truncate( txn, &num_deleted, 0 );
			//handle ret!=0
			//blablabla...
			//txn->commit(0);
		}
		catch(DbException ex)
		{
			ret = ex.get_errno();
			cout << ex.what();
			cout << ex.get_errno();
			cout<<"Error. DiskBDB::Truncate-> Exception encountered while truncating DiskBDB for DZName: " << s_dzname << endl;
			//txn->abort();
		}
		
		DbHandle->close(0);
	}
		
	if(ret!=0)
	{
		cout<<"The Handle was not retrieved from datazone manager"<<endl;
		return ret;
	}

	return ret;
}

int DiskBDB::Get (const Vdt& dzname, const Vdt& Key, Vdt *Value,stats *stats_update)
{
	Db *DbHandle;
	Dbt dbt_value;
	int ret=0;
	//ret=m_dzManager->GetHandleForPartition("B0",2,DbHandle);
	ret=m_dzManager->Get(dzname,Key,DbHandle);
	if(ret!=0)
	{
		cout<<"Datazone handle not retrieved from datazone manager"<<endl;
		return ret;
	}

	Dbt key(Key.get_data(),Key.get_size());
	dbt_value.set_data((*Value).get_data());
	dbt_value.set_size((*Value).get_size()); 
	dbt_value.set_ulen((*Value).get_size() );
	dbt_value.set_flags( DB_DBT_USERMEM );

	bool keepTrying=true;
	int numTries=0;
	while(keepTrying && numTries < m_maxDeadlockRetries)
	{
		numTries++;
		try {
			ret=DbHandle->get(NULL,&key,&dbt_value,0);
			keepTrying=false;
			if( ret == 0 )
				Value->set_size( dbt_value.get_size() );
		}
		catch(DbException &e) {
			if(numTries==1)
				printf("DiskBDB Get::%s\n",e.what()); 
			
			ret=e.get_errno();

			if(ret==DB_LOCK_DEADLOCK)
			{
				if(stats_update)
					stats_update->NumDeadlocks++;
			}
			else
				keepTrying=false;

			if( ret==DB_BUFFER_SMALL )
				(*Value).set_size(dbt_value.get_size());
		} catch(exception &e) {
			cout << e.what() << endl;
			return (-1);
		}   
	}
	return ret;
}

int DiskBDB::Shutdown()
{
	return 0;
}
