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

#pragma once
#include "WLGCommon.h"
#include <iostream>
#include <windows.h>
#include "WLGGetOneTraceElt.h"
#include "PoissonD.h"
using namespace std;

enum CmndType { Get, Insert, Delete};

struct OneRequest{  //one request, example: Get,2343,5,50
		CmndType cmnd;
		int key;
		int zoneid;
		int size;
		int TimeForReq;
	};

struct ListElt{
		OneRequest aRequest;
		struct ListElt *next;
	};



struct LastResultThr{
	float TQT; //Total Q-Time
	float TST; //Total Service-Time
	int TN; //Total Requests which this thread handle

};

//this class is read the request from the trace file, put them in the list and then send the request of one batch to the system and caculate the time...

class WLGList
{
public:
	

	ListElt *p, *t;
	WLGList(void);
	void FirstGenerate(); 
	void GenerateRequest(OneRequest *p);  //read from the file and return the next request
	void AddToList(ListElt *e);       //its input is the next elemen of the list
	~WLGList(void);
	int WLGList::getNumofLoaded(void);
	int coldStartReq(OneRequest *p);
	ListElt *getFreeElt(void);
	ListElt *GetPopulatedElt(void);	
	void freePopulatedElt(ListElt *e);
	void WLGList::GenerateOneRequest(ListElt *p);
	bool isFinish(void);
	void startAgain(void);
	int TraceEltsProcessed();

	int num_of_populated_elt;
	int last_try_elt;
	void reset(void);


	
private:
	int num_of_loaded_request;
	char *TF;
	int num_trace_elts;
	WLGGetOneTraceElt *gote;
	ListElt Elts[MAXA];
	ListElt *freeList;
	int totalReadFromFile;
	CRITICAL_SECTION FreeList;
	CRITICAL_SECTION PrefetchList;
	int ArrivalTime;
	PoissonD *poi;
};

