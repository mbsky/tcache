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


#ifndef GET_ONE_TRACE_ELT_H__
#define GET_ONE_TRACE_ELT_H__

//#pragma once
#include <iostream>
#include <fstream>
#include <string>

using namespace std;
class WLGGetOneTraceElt
{
private:
	ifstream Tfile;
	string line;
	int dzid, key, sz; //zoneid, key, size
	char cmnd;
	string::iterator it;
	char m_separator;

	int WLGGetOneTraceElt::RefreshLine(void);
	int WLGGetOneTraceElt::ParseLine(void);
	int WLGGetOneTraceElt::GetIntToken(void);
	int WLGGetOneTraceElt::GoToNextComma(void);
	int WLGGetOneTraceElt::GetCmdChar(void);


public:
	int WLGGetOneTraceElt::GetNext();
	char WLGGetOneTraceElt::GetCommand(void);
	int WLGGetOneTraceElt::GetDZid(void);
	int WLGGetOneTraceElt::GetSZ(void);
	int WLGGetOneTraceElt::GetKey(void);
	int WLGGetOneTraceElt::ShutDown(void);
	void WLGGetOneTraceElt::reread(void);

	WLGGetOneTraceElt( const std::string filename, char separator );
	~WLGGetOneTraceElt(void);
};

#endif // GET_ONE_TRACE_ELT_H__
