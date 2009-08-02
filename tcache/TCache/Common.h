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

#ifndef	__TC_COMMON_H_
#define __TC_COMMON_H_

#include "stdafx.h"
#include "db_cxx.h"
#include "Vdt.h"
#include <string>
#include <iostream>
#include <sstream>
#include <float.h>



#define TC_CONCAT_SEP			'#'
#define TC_CONCAT_SEP_Q		'$'
#define TC_CACHE_DATA_REC_KEY_HEADER		'D'
#define TC_CACHE_MD_LOOKUP_KEY_HEADER		'Q'
#define TC_TRUCATE_DZONE_MASK		"_trunc"

// Constant values
#define TC_USER_OBJECT_BUFFER_SIZE		1 * 1024 * 1024
#define TC_MAX_CATALOGE_VALUE_SIZE		16
#define TC_MAX_CATALOGE_KEY_SIZE		256
#define TC_MAX_KEY_SIZE		50
#define TC_LOG_BUFFER_SIZE		16 * 1024 * 1024	// 8 MB
#define TC_SM_BUFFER_SIZE		128		// bytes for char array used for generating DBTs
#define TC_LARGE_BUFFER_SIZE	1024
#define TC_VALUE_BUFFER_SIZE	92 * 1024 * 1024
#define TC_HISTORY_Q_VAL		DBL_MAX

#define TC_DEADLOCK_NUMBER_OF_TRIES	10
#define TC_MAX_RETRIES					10
#define TC_NUM_CURSOR_EVICTIONS		4

#define TC_VICTIMIZE_BY_SIZE		0x00010000
#define TC_VICTIMIZE_BY_RECORD		0x00020000

#define TC_RENAME_DZ_TRUNCATE		0x00000010
#define TC_RENAME_DZ_ORIGINAL		0x00000020

#define TC_DELDZKEYS_RANGE_MD		0x00200000
#define TC_DELDZKEYS_RANGE_DATA	0x00300000

#define TC_CACHE_KEY_ADMITTED			0x01100000
#define TC_CACHE_KEY_NOT_ADMITTED		0x02200000

#define TC_FLAG_MAINTAIN_MD			0x01110000
#define TC_FLAG_MAINTAIN_NO_MD			0x02220000

#define TC_CLEAN_FLAG		'C'
#define TC_DIRTY_FLAG		'D'
#define	TC_HISTORY_FLAG	'H'
#define TC_FORCE_WRITE_FLAG	'F'


#define TC_VERBOSE	1		// Toggle debug print messages, 0 for false, 1 for true
#define _TC_USE_MEM	1	// Toggle use of memory caching. 1 for yes, 0 to only use the disk BerkeleyDB
#define TC_DEBUG	0
#define TC_LOCK_DEBUG	1

#define TC_TRANSACTIONAL_REPDB	1		// 0 for false, 1 for true

// Database names and filenames
#define TC_DATAZONE_MANAGER_FNAME	"TC__Datazones.db"
#define TC_GENERIC_DBNAME			"_TC_Generic"
#define TC_ORDERED_QVALS_DBNAME	"TC_ORDERED_QVALS"
#define	TC_MEMORY_DBNAME			"TC_INMEMORY_DB"
#define TC_DZDBMAP_NAME			"TC_DZDBMAP_DB"
#define TC_DATAINDEX_DB			"TC_DATAINDEX_DB"
#define TC_L_VALUE_DB				"TC_L_VALUE_DB"

// Error codes
#define	TC_INDEX_NOT_FOUND		5
#define TC_DATAZONE_NOT_FOUND	6
#define TC_MEMORY_FULL			0x00010000
#define TC_BAD_ARRAYSIZE		0x00020000
#define TC_DATAZONE_ALREADY_EXISTS	0x00040000
#define TC_FAILED_MEM_FULL		0x00080000
#define TC_SYSTEM_SHUTDOWN		0x00100000
#define TC_DELETE_MD_NOT_FOUND	0x00001100

//BDB Error code
#define BDB_FAILED_MEM_FULL		-31000

// Eviction codes
#define	TC_EVICT_MORE_SPACE_REQD	11		// TODO: Might not be necessary


#define uint	unsigned int
#define uchar	unsigned char

#define PARTITION_INVALID						-666
#define TC_MAX_QUEUED_INSERTS					1000000	// Used for max count for the semaphore
//#define MAX_SYNCHRONOUS_INSERT					8		// Used for max number of synchronous inserts
#define TC_ADMISSION_CONTROL_FAILURE			-64
#define TC_DIRTY_CLEAN_RATIO_FAILURE			-65
#define TC_MIN_COMPRESSION_SIZE				500
#define TC_MIN_COMPRESSION_RATIO				0.75
#define TC_COMPRESSION_FLAG					'C'
#define	TC_UNCOMPRESSION_FLAG					'U'
#define TC_DB_BUFFER_SMALL_RETRY				10

#define DTS(x) TCUtility::doubleToString(x)
#define ITS(x) TCUtility::intToString(x)


enum IndexType
{
    INDEX_HASH  ,
    INDEX_B_TREE,
    INDEX_BITMAP,
    INDEX_R_TREE
};

enum Location
{
    DRAM,
    FLASH,
    DISK
};

class _export TCUtility
{
public:
	static std::string concatKeyDatazone(	const std::string &datazoneName, 
											const std::string &key );

	static void concatVdts(  const Vdt &Vdt1,
									const Vdt &Vdt2,
									char* buffer,
									int &size );

	static void concatKeyDatazone(  const Vdt &dzName,
									const Vdt &key,
									char* buffer,
									int &size );

	static void concatKeyDatazoneQ( const Vdt &dzName,
									const Vdt &key,
									char* buffer,
									int &size );

	static int splitKeyDatazonePair(	const std::string &datazone_key_pair,
										std::string &datazoneName,
										std::string &key );

	static int splitKeyDatazonePair(	const Vdt &datazone_key_pair,
										Vdt &datazoneName,
										Vdt &key );

	static int splitKeyDatazonePairQ(	const Vdt &datazone_key_pair,
										Vdt &datazoneName,
										Vdt &key );

	static int fixDataDirectoryPath(	const std::string &dataDir,
										std::string &fixPath);

	static int compare( const Vdt &vdt1, const Vdt &vdt2 );

	static void printDBContents( Db* dbp );

	static void checkStats( Db* dbp, DBTYPE type );

	static Vdt convertToVdt( const std::string &name );

	inline static int round( const double &val );

	// Convert an integer to a string 
	static std::string intToString( const int &number );

	static std::string doubleToString( const double &number );

	static std::wstring stringToWideString( const std::string &str );

	static std::string wideStringToString( const std::wstring &wstr );

	static int bernsteinHash (const Vdt &dzname, const Vdt &key,const int moduloVal);

	static int sumHash (const Vdt &dzname, const Vdt &key,const int moduloVal);

	static int bernsteinHash (const char *key,const int length,const int moduloVal);

	static Vdt cstringToVdt (const char *cstring);

};

#endif // __TC_COMMON_H_
