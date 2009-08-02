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

#ifndef __DATAZONE_MANAGER_H_
#define __DATAZONE_MANAGER_H_

#include "Vdt.h"
#include "db_cxx.h"
#include "Common.h"
#include "ConfigParameters.h"
#include "HashIdx.h"
#include <string>

using namespace std;

#define DM_CATALOG "__catalog.db"

// Keeps track of Db* for each datazone on Disk using the Hash Index
// Manages the catalog for the number of partions per datazone
class _export DatazoneManager
{
private:
	
	Db* m_catalog;
	DbEnv* m_diskDBEnv;
	HashIdx *m_HI;
	char m_dzNameBuffer[TC_LARGE_BUFFER_SIZE];
	int m_dzNameBufferPos;
	int m_dzNameBufferSize;
	int	m_dbPageSize;
	short m_numOfPartitions;


	int OpenHandle(const char *dzname, const int length,Db* &dbptr);
	int RenameDZFromCataloge( const Vdt& dzName, const int renameFlag );
	
public:
	int AppendPartitionID(const Vdt&dzname, const int PartionID,string &dzName);
	int GetHandleForPartition(const char *dzname, const int lenght, Db* &DbHandle);

	int Initialize( DbEnv* diskEnvironment, const ConfigParametersV& cp );
	int Shutdown();
	int Create( const Vdt &dzName );
	int Get( const Vdt &dzName, const Vdt &Key, Db* &retDBptr );
	int Exists( const Vdt &dzName );
	int CloseHandle(const Vdt&dzname,const int PartitionID);
	int CloseHandle( const Vdt &dzName, const Vdt &Key);
	inline int GetPartitionID(const Vdt& dzname, const Vdt&key);
	short GetNumPartition(const Vdt &dzname);
	int getNumDatazones();
	void _deleteAll( void );
	int GetDBCursor( Dbc* &cursorp ) const;
	int GetAllDatazones( Vdt* dzNameArr, int* NumberOfDataZones );
	bool checkIfHandleExistsInHashIndex(const Vdt&dzname, const Vdt &Key);
	int OpenHandleNoEnv(const char *dznameFilePath, int length, Db* &dbptr);
	int Rename( const Vdt& dzName, const int renameFlag );

};

#endif //  __DATAZONE_MANAGER_H_