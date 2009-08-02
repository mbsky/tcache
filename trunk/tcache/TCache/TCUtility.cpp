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
#include "Common.h"


std::string TCUtility::concatKeyDatazone(	const std::string &datazoneName, 
										const std::string &key )
{
	return datazoneName + TC_CONCAT_SEP + key;
}

void concatVdts(  const Vdt &Vdt1,
								const Vdt &Vdt2,
								char* buffer,
								int &size )
{
	size = 0;
	memcpy( buffer, Vdt1.get_data(), Vdt1.get_size() );
	size += Vdt1.get_size();
	//buffer[size] = TC_CONCAT_SEP;
	//size++;
	memcpy( buffer + size, Vdt2.get_data(), Vdt2.get_size() );
	size += Vdt2.get_size();
}


void TCUtility::concatKeyDatazone(  const Vdt &dzName,
								const Vdt &key,
								char* buffer,
								int &size )
{
	size = 0;
	memcpy( buffer, dzName.get_data(), dzName.get_size() );
	size += dzName.get_size();
	buffer[size] = TC_CONCAT_SEP;
	size++;
	memcpy( buffer + size, key.get_data(), key.get_size() );
	size += key.get_size();
}

void TCUtility::concatKeyDatazoneQ( const Vdt &dzName,
								const Vdt &key,
								char* buffer,
								int &size )
{
	size = 0;
	memcpy( buffer, dzName.get_data(), dzName.get_size() );
	size += dzName.get_size();
	buffer[size] = TC_CONCAT_SEP_Q;
	size++;
	memcpy( buffer + size, key.get_data(), key.get_size() );
	size += key.get_size();
}

int TCUtility::fixDataDirectoryPath(	const std::string &dataDir,
										std::string &fixPath)
{
	size_t endchar_pos = dataDir.find_last_of("/\\");
	size_t path_lenght = dataDir.length();	
	if( endchar_pos != (path_lenght-1) )
	{
		std::string endchar = dataDir.substr(endchar_pos, 1);
		fixPath = dataDir;
		fixPath += endchar;
		if(endchar.compare("\\") == 0 )
			fixPath += endchar;
	}

	return 0;
}

int TCUtility::splitKeyDatazonePair(	const std::string &datazone_key_pair,
									std::string &datazoneName,
									std::string &key )
{
	size_t index = datazone_key_pair.find_first_of( TC_CONCAT_SEP );

	if( index == datazone_key_pair.npos )
		return 1;

	datazoneName = datazone_key_pair.substr( 0, index );
	key = datazone_key_pair.substr( index + 1, datazone_key_pair.length() );

	return 0;
}

int TCUtility::splitKeyDatazonePair(	const Vdt &datazone_key_pair,
									Vdt &datazoneName,
									Vdt &key )
{		
	std::string dz_key_pair;
	dz_key_pair.insert( 0, (char*)datazone_key_pair.get_data(), 
		datazone_key_pair.get_size() );

	size_t index = dz_key_pair.find_first_of( TC_CONCAT_SEP );

	if( index == dz_key_pair.npos )
		return 1;

	datazoneName.set_data( (char*) datazone_key_pair.get_data() );
	datazoneName.set_size( (int) index );

	//key = dz_key_pair.substr( index + 1, dz_key_pair.length() );
	index++; // Account for separator char
	key.set_data( (char*) datazone_key_pair.get_data() + index );
	key.set_size( datazone_key_pair.get_size() - int(index) );

	return 0;
}

int TCUtility::splitKeyDatazonePairQ(	const Vdt &datazone_key_pair,
									Vdt &datazoneName,
									Vdt &key )
{		
	std::string dz_key_pair;
	dz_key_pair.insert( 0, (char*)datazone_key_pair.get_data(), 
		datazone_key_pair.get_size() );

	size_t index = dz_key_pair.find_first_of( TC_CONCAT_SEP_Q );

	if( index == dz_key_pair.npos )
		return 1;

	datazoneName.set_data( (char*) datazone_key_pair.get_data() );
	datazoneName.set_size( (int) index );

	//key = dz_key_pair.substr( index + 1, dz_key_pair.length() );
	index++; // Account for separator char
	key.set_data( (char*) datazone_key_pair.get_data() + index );
	key.set_size( datazone_key_pair.get_size() - int(index) );

	return 0;
}

int TCUtility::compare( const Vdt &vdt1, const Vdt &vdt2 )
{
	if( vdt1.get_size() - vdt2.get_size() != 0 )
		return vdt1.get_size() - vdt2.get_size();

	std::string vdt1_str = ""; 
	std::string vdt2_str = "";

	vdt1_str.insert( 0, (char*) vdt1.get_data(), vdt1.get_size() );
	vdt2_str.insert( 0, (char*) vdt2.get_data(), vdt2.get_size() );

	return vdt1_str.compare( vdt2_str );
}

void TCUtility::printDBContents( Db* dbp )
{
	std::string temp_key;
	int temp_int;

	Dbc *cursorp;
	Dbt key(NULL,0), value(NULL,0);

	dbp->cursor( NULL, &cursorp, 0 );

	while( cursorp->get( &key, &value, DB_NEXT ) == 0 )
	{
		//memset( temp_keyc, 0, 32 );
		//strncpy( temp_keyc, (char*) key.get_data(), key.get_size() );
		//temp_key = temp_keyc;		
		
		//temp_key.insert( 0, (char*) key.get_data(), key.get_size() );

		//temp_key = "";
		//temp_key = intToString( *(int*) key.get_data() );

		temp_int = *(int*) key.get_data();

		//if( temp_int > 14600 )
			std::cout<< temp_int << "; ";
	}

	cursorp->close();

	//cout<< endl;
}

void TCUtility::checkStats( Db* dbp, DBTYPE type )
{
	if( type == DB_HASH )
	{
		DB_HASH_STAT* stats = 0;
		dbp->stat( NULL, &stats, 0 );
		delete stats;
	}
	else if( type == DB_BTREE )
	{
		DB_BTREE_STAT* stats = 0;
		dbp->stat( NULL, &stats, 0 );
		delete stats;
	}
}

Vdt TCUtility::convertToVdt( const std::string &name )
{
	Vdt result;
	result.set_data( (char*) name.c_str() );
	result.set_size( (int)name.length() );

	return result;
}

inline int TCUtility::round( const double &val )
{
	return int(val + 0.5);
}

// Convert an integer to a string 
std::string TCUtility::intToString( const int &number )
{
	std::ostringstream oss;

	// Works just like cout
	oss<<  number;

	// Return the underlying string
	return oss.str();
}


// Convert an integer to a string 
std::string TCUtility::doubleToString( const double &number )
{
	std::ostringstream oss;

	// Works just like cout
	oss<<  number;

	// Return the underlying string
	return oss.str();
}

int TCUtility::bernsteinHash (const Vdt &dzname, const Vdt &key,const int moduloVal)
{
    char *Dzname= (char *) dzname.get_data();
    char *Key = (char*) key.get_data();

    int retHash=5381;
    int len=0;

    while(len<dzname.get_size())
    {
        retHash = ((retHash << 5) + retHash) + *Dzname++;
        len++;
    }

    len=0;
    while(len<key.get_size())
    {
        retHash = ((retHash << 5) + retHash) + *Key++;
        len++;
    }   
   
    if (retHash < 0) retHash = -retHash;
    return retHash%moduloVal;
}

int TCUtility::sumHash (const Vdt &dzname, const Vdt &key,const int moduloVal)
{
    char *Dzname= (char *) dzname.get_data();
    char *Key = (char*) key.get_data();
   
    int retHash=0;
    int len=0;
    while(len<dzname.get_size())
    {
        retHash+=*Dzname++;
        len++;
    }
    len=0;
    while(len<key.get_size())
    {
        retHash+=*Key++;
        len++;
    }
    if (retHash < 0) retHash = -retHash;
    return retHash%moduloVal;
}

int TCUtility::bernsteinHash (const char *key,const int length,const int moduloVal)
{
	int retHash=5381;
    int len=0;

    while(len<length)
    {
        retHash = ((retHash << 5) + retHash) + *key++;
        len++;
    }   
   
    if (retHash < 0) retHash = -retHash;
    return retHash%moduloVal;
}

std::wstring TCUtility::stringToWideString(const std::string& str)
{
    int len;
    int slength = (int)str.length() + 1;
    len = MultiByteToWideChar(CP_ACP, 0, str.c_str(), slength, 0, 0); 
    wchar_t* buf = new wchar_t[len];
    MultiByteToWideChar(CP_ACP, 0, str.c_str(), slength, buf, len);
    std::wstring r(buf);
    delete[] buf;
    return r;
}

std::string TCUtility::wideStringToString( const std::wstring &wstr )
{
	std::string str( wstr.length(), ' ' );
	std::copy( wstr.begin(), wstr.end(), str.begin() );
	return str;
}

Vdt TCUtility::cstringToVdt (const char *cstring)
{
	Vdt data((char *) cstring,strlen(cstring)+1);
	return data;
}