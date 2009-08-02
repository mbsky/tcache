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

#ifndef __CACHING_ALGORITHM_H__
#define __CACHING_ALGORITHM_H__

#include <ctime>
#include <list>
#include <iostream>
#include "Windows.h"
#include "Vdt.h"
#include "db_cxx.h"
#include "Common.h"
#include "ConfigParameters.h"

struct _export CachingAlgorithmInputData
{
	const Vdt	*dzName; 
	const Vdt	*Key; 
	const Vdt	*value; 
	double		cost;
};



class _export CachingAlgorithm
{
public:
	const static int CA_FLAG_ADMISSION_CONTROL = 0x00000002;
	const static int CA_FLAG_MAINTAIN_HISTORY = 0x00000004;
	const static int CA_FLAG_SAME_Q_NO_UPDATE = 0x00000008;
	const static int CA_FLAG_WITHIN_DELTA_NO_UPDATE = 0x00000010;
	const static int CA_FLAG_LARGER_THAN_AVG_Q_NO_UPDATE = 0x00000020;

	
protected:
	// Size of the metadata block.
	int m_dataSize;

	// Bitwise flags
	int m_flags;

public:
	CachingAlgorithm( const ConfigParametersV* cp = NULL );

	// Determine the Q value (used to determine if an object should stay in the cache. the lower the Q
	//  value, the more likely the object will be evicted from the cache).
	// Metadata contains a block of data with all the relevant information for the particular
	//  algorithm as dictated by GenerateMetadata and UpdateRead/Write.
	// L_value holds the last Q value evicted and can be used to influence the Q value calculation
	//  as dictated by the algorithm.
	virtual double calcQValue( const void* metadata, const double &L_value ) = 0;

	// When an item is seen for the first time, this function will generate the associated metadata
	//  in the buffer provided by the user at the metadata ptr.
	// Returns the size of the metadata block, which is the same as GetDataSize()
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size )  = 0;

	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size ) { return -1; }

	// Update the metadata based on Read or Write operation. 2 separate functions to allow for
	//  the algorithm to differentiate between Reads and Writes
	// Writes might also change the data of the object and thus, the corresponding
	//  metadata information as well if the algorithm dictates it.
	// Assumes that the user passes in a pointer to a block of valid memory that 
	//  contains the expected metadata values
	virtual int UpdateWrite( void* &metadata ) = 0;
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata ) = 0;
	virtual int UpdateRead( void* &metadata ) = 0;

	// The calling layer needs to know the size of the metadata block it needs to store
	// NOTE: The size of the block is expected to remain the same. Internally the algorithm can decide to
	//		 use fewer bytes but it should keep and return a constant data size to the 
	//		 calling layer.
	virtual int GetDataSize() = 0;

	// Specifes to the system which flags are active. Influences how the system handles
	//  the metadata and general background work to support the algorithm functions
	int GetFlags() { return m_flags; }


	// Performs checks to see if the current data item which is already in cache
	//  should be updated. Different algorithms will have different flags set, thus
	//  will only execute the checks which are toggled ON by their flags.
	// (Ex: Only an algorithm with CA_FLAG_WITHIN_DELTA_NO_UPDATE set will check
	//  if the current item has been updated recently and return true to indicate that 
	//  it should not be updated)
	bool NoNeedToUpdateMetadata(	const double& oldQ, const double& newQ, const double& avgQ,
									const LARGE_INTEGER& oldTimestamp,
									const LARGE_INTEGER& currentTimestamp,
									const LARGE_INTEGER& updateDelta );

	
	// Admission control. L value holds the last Q value evicted and is used to determine if
	//  current item should be admitted into the cache
	virtual bool AdmitKey( const double& currQValue, const double& L_value ) = 0;

	// Debugging function that outputs the contents of a metadata chunk to cout
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const = 0;
};


class _export GreedyDualSizeAlgorithm : public CachingAlgorithm
{
protected:
	// Metadata that applies to all records
	

	// Metadata about the current record
	double md_cost;			// Cost associated with retrieving the object from external storage layer
	int md_record_size;		// Size of the record in the cache
	

public:
	GreedyDualSizeAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize; }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


class LRUAlgorithm : public CachingAlgorithm
{
protected:
	// Metadata that applies to all records
	volatile long md_g_counter;

	// Metadata about the current record
	int md_timestamp;
	

public:
	LRUAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return sizeof(md_timestamp); }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


class LRUTimeAlgorithm : public CachingAlgorithm
{
protected:
	LARGE_INTEGER getTime();
	// Metadata that applies to all records

	// Metadata about the current record
	LARGE_INTEGER md_timestamp;	

public:
	LRUTimeAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize; }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


class LRU2Algorithm : public CachingAlgorithm
{
protected:
	// Metadata that applies to all records
	volatile long md_g_counter;

	// Metadata about the current record
	int md_timestamp1;		// Newer timestamp
	int md_timestamp2;		// Older timestamp

	void incrementCounter();

public:
	LRU2Algorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return sizeof(md_timestamp1) + sizeof(md_timestamp2); }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


class LRU2TimeAlgorithm : public CachingAlgorithm
{
protected:
	// Metadata that applies to all records
	LARGE_INTEGER md_g_basetime;		// Keeps track of the starting time
	LARGE_INTEGER m_ticks_per_msec;		// Keeps track of timer frequency in milliseconds

	// Metadata about the current record
	LARGE_INTEGER md_timestamp1;		// Newer timestamp
	LARGE_INTEGER md_timestamp2;		// Older timestamp

	LARGE_INTEGER getTime();

public:
	LRU2TimeAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize; }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};



class IntervalBasedGreedyDualAlgorithm : public CachingAlgorithm
{
protected:
	// Metadata that applies to all records	
	volatile long md_g_counter;

	// Metadata about the current record
	int md_timestamp1;		// Newer timestamp
	int md_timestamp2;		// Older timestamp
	int md_references;		// Number of references since item was cached
	int md_record_size;		// Size of the whole record
	double md_cost;			// Cost associated with retrieving the object from external storage layer

	void incrementCounter();

public:
	IntervalBasedGreedyDualAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize; }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


class IntervalBasedGreedyDualTimeAlgorithm : public CachingAlgorithm
{
protected:
	// Gets the current time and increments the Aging counter if necessary
	LARGE_INTEGER getTime();

	// Metadata that applies to all records
	LARGE_INTEGER md_g_basetime;		// Keeps track of the starting time
	LARGE_INTEGER m_ticks_per_msec;		// Keeps track of timer frequency in milliseconds

	// Metadata about the current record
	LARGE_INTEGER md_timestamp1;		// Newer timestamp
	LARGE_INTEGER md_timestamp2;		// Older timestamp
	int md_references;		// Number of references since item was cached
	int md_record_size;		// Size of the whole record
	double md_cost;			// Cost associated with retrieving the object from external storage layer

public:
	IntervalBasedGreedyDualTimeAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize;	}
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};





class LRU_SKAlgorithm : public CachingAlgorithm
{
protected:
	// Metadata that applies to all records
	volatile long md_g_counter;

	// Metadata about the current record
	int md_timestamp1;		// Newer timestamp
	int md_timestamp2;		// Older timestamp
	int md_record_size;		// Size of the whole record
	double md_cost;			// Cost associated with retrieving the object from external storage layer

	void incrementCounter();

public:
	LRU_SKAlgorithm( const ConfigParametersV* cp = NULL);
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize; }
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


class LRU_SKTimeAlgorithm : public CachingAlgorithm
{
protected:
	// Gets the current time and increments the Aging counter if necessary
	LARGE_INTEGER getTime();

	// Metadata that applies to all records
	LARGE_INTEGER md_g_basetime;		// Keeps track of the starting time
	LARGE_INTEGER m_ticks_per_msec;		// Keeps track of timer frequency in milliseconds

	// Metadata about the current record
	LARGE_INTEGER md_timestamp1;		// Newer timestamp
	LARGE_INTEGER md_timestamp2;		// Older timestamp
	int md_record_size;		// Size of the whole record
	double md_cost;			// Cost associated with retrieving the object from external storage layer

public:
	LRU_SKTimeAlgorithm( const ConfigParametersV* cp = NULL );
	virtual double calcQValue( const void* metadata, const double &L_value );
	virtual int GenerateMetadata( const Vdt& dzName, const Vdt &Key, const Vdt *value, void* &metadata, int &metadata_size );
	virtual int GenerateMetadata( const CachingAlgorithmInputData& inputData, void* &metadata, int &metadata_size );
	virtual int UpdateWrite( const CachingAlgorithmInputData& inputData, void* &metadata );
	virtual int UpdateWrite( void* &metadata );
	virtual int UpdateRead( void* &metadata );
	virtual int GetDataSize() { return m_dataSize;	}
	virtual bool AdmitKey( const double& currQValue, const double& L_value );
	virtual void printMetadata( const void* metadata, std::ostream &out, const char& separator = ',' ) const;
};


#endif // __CACHING_ALGORITHM_H__