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
#include "WLGList.h"
#include "WLGCommon.h"
#include <iostream>
#include <fstream>
#include "WLGGetOneTraceElt.h"
#include "PoissonD.h"
using namespace std;

WLGList::WLGList(void)
{
	
	WLGList::ArrivalTime=0;
	p=NULL;
	t=NULL;
	poi = NULL;
	WLGList::num_of_populated_elt =0;
	WLGList::last_try_elt = 0;

	WLGList::num_of_loaded_request = 0;
	WLGList::totalReadFromFile = 0;
	WLGList::num_trace_elts = 0;
	WLGList::TF = TraceDirectory;
	//ListElt *q,*t;   // in out linklist, at the end of each put or get, p in the head, t in the tail 

	if (*Distribution == 'p')
		poi=new PoissonD(Num_of_req,Granularity); 
	
	InitializeCriticalSection( &FreeList );
	InitializeCriticalSection( &PrefetchList );

	//Setup the free list so that it can be populated with elements.
	freeList = &Elts[0];
	for(int i=0; i< MAXA-1 ;i++)
	{
		Elts[i].next = &Elts[i+1];
	}
	Elts[MAXA-1].next = NULL;



	//Initialize the trace file reader
	gote = new WLGGetOneTraceElt(TF,',');
}

int WLGList::TraceEltsProcessed()
{
	return num_trace_elts;
}

ListElt* WLGList::getFreeElt()
{
	ListElt *temp;
	EnterCriticalSection(&FreeList);

	if (freeList == NULL)
	{
		printf("Error in WLGList.cpp::getFreeElt - freeList is null; this should not have happened!  Continuing.  However, the experiment is now invalid.\n");
		LeaveCriticalSection(&FreeList);
		return NULL;
	}



	temp = freeList;
	freeList = freeList->next;

	LeaveCriticalSection(&FreeList);

	return temp;
	
}


void WLGList::FirstGenerate()
{
	counter =0;
	ArrivalTime = 0;
	
	if (*Distribution == 'p'){
		if (poi != NULL){ 
			delete poi;
			poi = NULL;
		}

		poi = new PoissonD(Num_of_req, Granularity);
	}
	else poi = NULL;

	WLGList::ArrivalTime=0;
	ListElt *oneElt;
	WLGList::num_of_loaded_request = 0;
	WLGList::totalReadFromFile = 0;
	for(int i=0; i< NumPrefetchUnits; i++)
	{
		oneElt = getFreeElt();
		GenerateOneRequest(oneElt);
		AddToList(oneElt);
		

	}


}

int WLGList::getNumofLoaded(void){
	return num_of_loaded_request;
}


int WLGList::coldStartReq(OneRequest *p)
{
	//WLGList::num_of_loaded_request++;
	WLGList::totalReadFromFile++;

	GenerateRequest(p);
	return 1;	
}


void WLGList::GenerateOneRequest(ListElt *p)
{
	
	WLGList::totalReadFromFile++;
	GenerateRequest( &(p->aRequest));
	return;
}



void WLGList::AddToList(ListElt *e){//its input is the next elemen of the list

	EnterCriticalSection(&PrefetchList);
	e->next = NULL;

	if(t==NULL && p==NULL)
	{
		p = t = e;
	}
	else
	{
		t->next = e;
		t=e;
	}
	counter++;
	WLGList::num_of_loaded_request++;
	LeaveCriticalSection(&PrefetchList);
} 



ListElt *WLGList::GetPopulatedElt(void)
{
	EnterCriticalSection(&PrefetchList);
	ListElt *temp;
	if (p ==NULL)
	{
		printf("Error in WLGList.cpp::GetPopulatedElt - p is null; this should not have happened!  Continuing.  However, the experiment is now invalid.\n");
		LeaveCriticalSection(&PrefetchList);
		return NULL;
	}
	temp = p;
	p = p->next;
	WLGList::num_of_populated_elt++;
	LeaveCriticalSection(&PrefetchList);
	return temp;
	

}


void WLGList::freePopulatedElt(ListElt *e)
{
	EnterCriticalSection(&FreeList);
	e->next = freeList;
	freeList = e;
	WLGList::num_of_loaded_request--;
	LeaveCriticalSection(&FreeList); 
}

void WLGList::reset(){
	WLGList::num_of_loaded_request=0;
}

void WLGList::GenerateRequest(OneRequest *p){  //read from the file and return the next request
	
	
		  p->key = gote->GetKey();
		  p->size = gote->GetSZ();
		  p->zoneid = gote->GetDZid();

		  switch (gote->GetCommand())
			{
				case 'G':
					p->cmnd = Get;
					break;
				case 'S':
					p->cmnd = Insert;
					break;
				case 'D':
					p->cmnd = Delete;
					break;
				default :
					printf("Command %c is unknown.\n", gote->GetCommand());
					break;
			}

		  if (*Distribution == 'u')
		  {
			  
			  int step = (counter) / Num_of_req;
			  int count = (counter) % Num_of_req;
			  int inEachBatch = (Granularity / Num_of_req)*(count);
			  p->TimeForReq = step * Granularity + inEachBatch;
			
			  //printf("Assigned %d.\n",p->TimeForReq);
			  
		  }

		  else if (*Distribution == 'p')
		  {
			  if (poi == NULL)
			  {
				  printf("Error, poi cannot be NULL here.\n");
				  exit (-1);
			  }
			  ArrivalTime += poi->getNextArrivalTime();
			  p->TimeForReq = ArrivalTime;

		  }

		  if (gote->GetNext() < 0){
			  printf("End of trace file has been reached.\n");
				TraceEOF = true;
			} else num_trace_elts++;
}

void WLGList::startAgain(void){
	WLGList::totalReadFromFile = 0;
}

bool WLGList::isFinish(void){
	EnterCriticalSection(&PrefetchList);
	if (WLGList::p->next == NULL){
		LeaveCriticalSection(&PrefetchList);
		return true;
	}
	LeaveCriticalSection(&PrefetchList);
	return false;
}

WLGList::~WLGList(void)
{
	ListElt *q;
   if( p == NULL )
        return;

   while( p != NULL )
   {
      q = p->next;
      delete p;
      p = q;
   }
}







