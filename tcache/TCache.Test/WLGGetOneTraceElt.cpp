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
#include "WLGGetOneTraceElt.h"
#include "WLGList.h"

//#define TRACE_FILE_NAME "G:\\585\\Trace99\\99ObjsGD.Save"
//#define TRACE_FILE_NAME "G:\\585\\Trace99\\99Objs.Save"
//#define TRACE_FILE_NAME "C:\\jyap\\585\\Trace99\\Trace99.1KGet"
//#define TRACE_FILE_NAME "G:\\585\\Trace99\\Trace99.1KGet"
//#define TRACE_FILE_NAME "G:\\585\\Trace99\\Trace99.10KGet"
//#define TRACE_FILE_NAME "C:\\jyap\\585\\Trace99\\Trace99.10KGet"
//#define TRACE_FILE_NAME "C:\\jyap\\585\\Trace99\\Trace99.100KGet"
//#define TRACE_FILE_NAME "G:\\585\\Trace21\\21Objs.Save"
//#define TRACE_FILE_NAME "C:\\jyap\\585\\Trace21\\Trace21.1KGet"
//#define TRACE_FILE_NAME "G:\\585\\Trace21\\Trace21.1KGet"
//#define TRACE_FILE_NAME "G:\\585\\Trace21\\Trace21.10KGet"
//#define TRACE_FILE_NAME "G:\\585\\Trace21\\Trace21.100KGet"

int WLGGetOneTraceElt::RefreshLine()
{
	if (getline(Tfile, line))
		return 1;
	else return -1;
}

////////////////////////////
void WLGGetOneTraceElt::reread(void)
{
	line.begin();
	
}
///////////////////////////



int WLGGetOneTraceElt::GetCmdChar()
{
	//check if the request is get,insert or delete
	int Goffset = 0;
	int Ioffset = 0;
	int Doffset = 0;

	Goffset = (int)line.find_first_of("G", 0);
	Ioffset = (int)line.find_first_of("S", 0);
	Doffset = (int)line.find_first_of("D", 0);

	if (Goffset > 0){
		cmnd = 'G';
		return Goffset;
	}
	if (Ioffset > 0){
		cmnd = 'S';
		return Ioffset;
	}
	if (Doffset > 0){ 
		cmnd = 'D';
		return Doffset;
	}
	return -1;
}

int WLGGetOneTraceElt::GetIntToken()
{
	return 1;
}

int WLGGetOneTraceElt::ParseLine()
{
	//it should read a line and put the item in their variable
	//our request are like: (trs,4/19/2009,Get,1,246591,1765,0)  which is (N/A,N/A,cmd,zoneid,key,size,N/A)
	//int dzid, key, sz;
	int offset1;
	int cmndoffset = WLGGetOneTraceElt::GetCmdChar();
	int commaoffset = 0;
	if (cmndoffset > 0){ //it means that we have one type (get, insert, delete) request
	
		commaoffset = (int)line.find_first_of(m_separator, cmndoffset);
		offset1 = commaoffset+1;
		commaoffset = (int)line.find_first_of(m_separator, offset1);

		string myToken = line.substr(offset1, commaoffset-offset1);
		dzid = atoi(myToken.c_str());

		
		offset1 = commaoffset + 1;
		commaoffset = (int)line.find_first_of(m_separator, offset1);
		myToken = line.substr(offset1, commaoffset-offset1);
		key = atoi(myToken.c_str());

 		offset1 = commaoffset + 1;
		commaoffset = (int)line.find_first_of(m_separator, offset1);
		myToken = line.substr(offset1, commaoffset-offset1);
		sz = atoi(myToken.c_str());
	}
	return 1;
}

int WLGGetOneTraceElt::GetNext()
{
	if (WLGGetOneTraceElt::RefreshLine() > 0){
		if (line.c_str() == NULL)
			printf("line is null.\n");
		it = line.begin();
		WLGGetOneTraceElt::ParseLine();
		return 1;
	} else return -1;
}





char WLGGetOneTraceElt::GetCommand()
{
	return cmnd;
}

int WLGGetOneTraceElt::GetDZid()
{
	return dzid;
}

int WLGGetOneTraceElt::GetSZ()
{
	return sz;
}

int WLGGetOneTraceElt::GetKey()
{
	return key;
}


int WLGGetOneTraceElt::ShutDown()
{
	Tfile.close();
	return 1;
}
WLGGetOneTraceElt::WLGGetOneTraceElt( const std::string filename, char separator )
{
	m_separator = separator;
	Tfile.open(filename.c_str(), ifstream::in);

	if (WLGGetOneTraceElt::RefreshLine() > 0)
	{
		it = line.begin();
		WLGGetOneTraceElt::ParseLine();
	};
}





WLGGetOneTraceElt::~WLGGetOneTraceElt(void)
{
}
