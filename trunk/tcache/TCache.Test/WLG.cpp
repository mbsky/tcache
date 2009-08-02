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

// WLG.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "WLGCommon.h"
#include "WLGList.h"
#include <iostream>
#include <iomanip>
#include <fstream>
#include <stdio.h>
#include <windows.h>
#include <ctime>
#include "TCache.h"
#include "ConfigParameters.h"
#include "ConfigurationLoader.h"
#include "Vdt.h"
#include "PoissonD.h"

#define VERBOSE 0
#define MAXSAMPLES 10000
#define SERVICETIME 200

#define VCACHEMANAGER_RUN

using namespace std;

HANDLE            m_semaphore;

CRITICAL_SECTION	g_Stats_Update_CS;

LARGE_INTEGER	g_totalDelaySleepTime;
__int64			g_totalIntendedDelay;
volatile long	g_numRequestsProcessed;

// Keep track of last successful results
LARGE_INTEGER	g_lastSuccessfulTotalDelaySleepTime;
__int64			g_lastSuccessfulIntendedDelay;
long			g_lastSuccessfulNumRequestsProcessed;
bool			g_currentLambdaSuccess;

WLGList *l;
LastResultThr lrt[MAXTHREADS];
char* TraceDirectory;	    //trace file
int NumPrefetchUnits;       //A
int Num_of_req;             //number of request in each batch
int Min_Num_of_req;             //min number of request in each batch
int Max_Num_of_req;             //max number of request in each batch
int Granularity;            //time for the number-of-request in each batch (millisec) 
int QThreshold;
double ColdStartMultiplier;
char* Distribution;         //uniform or random
int Num_of_thrds;           //one thread (at first)
int Sim_Time=0;
int cacheSize = 4*1024*1024;  //1 Megabyte - this is to be replaced with a value read from the configuration file
bool TraceEOF= false;
bool minrepeat = false;
bool maxrepeat = false;
int Min;            
int Max; 
TCache* vcm;
const int dataBufferSize = 4*1024;
void callVChash(OneRequest var1, char* dataBuffer, int dataBufferSize);
const int WLG_NUMZONES = 3;
Vdt* WLG_DZ;
char* WLG_Filename;
bool first = true;
int counter;
int ST[MAXA];
//int idx = 0;

bool g_traceEOFerror;


int WLGmain(int argc, _TCHAR* argv[], const ConfigParametersV& cp, const ConfigParametersWLG& cpw );



LARGE_INTEGER frequency;



LARGE_INTEGER SimulationEndTime ;
DWORD ProdcuerWaitTime=INFINITE;


bool EndTimeHasArrived()
{
	LARGE_INTEGER currentTime;
	QueryPerformanceCounter( &currentTime );
	if (currentTime.QuadPart > SimulationEndTime.QuadPart)
	{
		return true;
	}
	return false;
}


/* THREAD
the main idea of thred in c++
its works like a function, we have some function that always run (they should do their job in a loop) 
and whenever they have the cpu time they really do their jobs.
so, we put a name for each of difrrent thread that we have and then put the HANDLE SyncEvent part that mainly its job is syncrise the threads
at the end of each function for thread, we have SetEvent(SyncEvent); which its job is: whenever a thread finishes its job it should call and ask for the next job.
*/

/*
in this program, we have 2 diffrent Type of thread, Consumer and Producer and we have one Producer and n (n>=1) Consumer
*/

DWORD WINAPI WorkerThreadProducer( LPVOID lpParam ) 
{
	//WLGList* l = ((WLGList*)lpParam);
	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread:  Failed to establish the synchronization mechanism.\n");
		return -1;
	}
	
	//start our job with this thread
	//its job is asking for the next request to become ready

	//LOCK
	bool Done2 = false;
	g_currentLambdaSuccess = false;

	int dwEvent;

	while (!Done2)
	{
		dwEvent=WaitForSingleObject(m_semaphore,ProdcuerWaitTime);
		switch (dwEvent)
		{

		case WAIT_OBJECT_0:

			if (EndTimeHasArrived() == true ) 
			{
				Done2 = true;
				g_currentLambdaSuccess = true;
			}
			else if( TraceEOF == TRUE )
			{
				g_traceEOFerror = true;
				Done2 = true;
			}
			else {
				ListElt *p = l->getFreeElt();
				if (p == NULL){
					printf("Error in WorkerThreadProducer:  Failed to obtain a free element from List.\n");
				} else {
					l->GenerateOneRequest(p);
					if (VERBOSE) printf("Free Element was for object %d.\n",p->aRequest.key);
					l->AddToList(p);
					//counter++;
				}
			}


			break;

		case WAIT_ABANDONED:
			Done2 = true;
			printf("The event was non-signaled.\n");
			break;

		case WAIT_TIMEOUT:
			printf("Time out for the semaphore.\n");
			Done2 = true;
			//m_logStream->print("The time-out interval elapsed, the event was non-signaled.\n");
			//timeout=true;
			break;
		}


	}
	//UNLUCK
	
	
	//finish our job with this thread


	//Signal the main thread that this WorkerThread is done with the current element
	SetEvent(SyncEvent);

	return 0;
}

void DoWork()
{

	Sleep(SERVICETIME);
}

DWORD WINAPI WorkerThreadConsumer( LPVOID lpParam ) 
{
	
	char dataBuffer[dataBufferSize];
	int ID = *(int *)lpParam;
	printf("Thread id = %d.\n",ID);

	//WLGList* l = ((WLGList*)lpParam);
	// I have access to an object of list... printf ("The value of A in list l is %d.\n",l->A);
	HANDLE SyncEvent = OpenEvent ( EVENT_ALL_ACCESS , false, (LPCWSTR) "SyncEvent" );
	if (!SyncEvent) {
		printf ("WorkerThread:  Failed to establish the synchronization mechanism.\n");
		return -1;
	}
	
	
	//// time
	//LARGE_INTEGER beginTime, endTime, duration, frequency;
	//QueryPerformanceCounter( &beginTime );
	//QueryPerformanceCounter( &endTime );
	//QueryPerformanceFrequency( &frequency );
	//duration.QuadPart = endTime.QuadPart - beginTime.QuadPart;
	//int seconds = duration.QuadPart / frequency.QuadPart /1000;
	////cout << seconds;
	LARGE_INTEGER duration;
	LARGE_INTEGER StartTime;
	LARGE_INTEGER StartServiceTime ;
	QueryPerformanceCounter( &StartTime );

	LARGE_INTEGER StartTimeThisRequestRun;


	bool Done=false;

	while (!Done){

		

		if(l->getNumofLoaded() == 0)
		{
			printf("Number of Advanced Scheduled Jobs %d should be larger.\n", NumPrefetchUnits);
			printf("Increase the constant MAXTHREADS to %d, recompile and run again.", NumPrefetchUnits+1);
			exit(1);	
		}
		else{

			InterlockedIncrement( &g_numRequestsProcessed );

			LARGE_INTEGER StartTimeThisRequestRun;
			QueryPerformanceCounter( &StartTimeThisRequestRun );
			LARGE_INTEGER EndTimeThisRequestRun;

			
			ListElt *r;
			r = l->GetPopulatedElt();
			int startThisReq = r->aRequest.TimeForReq;
			if (VERBOSE)
				cout <<endl << "the time which we read this request is: "<<StartTimeThisRequestRun.QuadPart/frequency.QuadPart <<endl;

			//float delay;

			//check if we start this request sooner than it should be issued or not, if yes, we should go to sleep to start on-time not sooner
			/*if (StartTimeThisRequestRun.QuadPart < (StartTime.QuadPart + (frequency.QuadPart*startThisReq)))
			{
				LARGE_INTEGER duration;
				duration.QuadPart = StartTime.QuadPart + (frequency.QuadPart*startThisReq) - StartTimeThisRequestRun.QuadPart;
				float mseconds = duration.QuadPart / frequency.QuadPart;
				Sleep ( mseconds);
			}*/

			////up to when we have time to do this request
			//long double L = StartTime.QuadPart + (frequency.QuadPart*startThisReq) + (frequency.QuadPart * Granularity);


			LARGE_INTEGER duration;

			
			//cout<<endl<<"from list : " << startThisReq;
			//cout<<endl<<"from list2 : " << frequency.QuadPart*startThisReq;
			//cout<<endl<<"Start : " << StartTime.QuadPart;
			//cout<<endl<<"Start this : " << StartTimeThisRequestRun.QuadPart -StartTime.QuadPart;
			//cout<<endl<<"Start this : " << StartTimeThisRequestRun.QuadPart -StartTime.QuadPart - frequency.QuadPart*startThisReq;

					
						
			duration.QuadPart = (StartTime.QuadPart + (frequency.QuadPart*startThisReq)) - StartTimeThisRequestRun.QuadPart;
			float delay = duration.QuadPart / frequency.QuadPart;

			//double delay = ((StartTime.QuadPart + frequency.QuadPart*double(startThisReq)) - StartTimeThisRequestRun.QuadPart;

			if (delay == 0 || delay > 0)
			{
				int sleep_delay = delay;

				// Keeping track of actual amount of time spent sleeping between batches
				LARGE_INTEGER timer_begin, timer_end, timer_sleep;
				QueryPerformanceCounter( &timer_begin );

				if(sleep_delay < 100)
				{
					bool end_sleep = false;
					//g_totalSleepEachTime.QuadPart=0;
					while(!end_sleep)
					{
						Sleep(0);
						QueryPerformanceCounter( &timer_sleep );

						if( ( (timer_sleep.QuadPart - timer_begin.QuadPart)/frequency.QuadPart ) >= sleep_delay )
						{
							end_sleep=true;							
						}
					}
				}
				else
				{	
					Sleep( sleep_delay );					
				}

				QueryPerformanceCounter( &timer_end );
				

				EnterCriticalSection( &g_Stats_Update_CS );
				g_totalDelaySleepTime.QuadPart += (timer_end.QuadPart - timer_begin.QuadPart);
				g_totalIntendedDelay += sleep_delay;				
				LeaveCriticalSection( &g_Stats_Update_CS );

				//cout << endl<< "delay : "<<delay;

				delay = 0;
				
			}
			else
			{
				delay = (double) StartTimeThisRequestRun.QuadPart - (StartTime.QuadPart + (frequency.QuadPart*startThisReq));
				delay = delay / frequency.QuadPart;
				//cout << endl<<"delay : "<<delay;
			}

			//cout<< endl<<endl << "delay for request " <<counter << " is " << delay;

			QueryPerformanceCounter( &StartServiceTime );

#ifdef VCACHEMANAGER_RUN
			callVChash(r->aRequest, dataBuffer, dataBufferSize); 
#else
			DoWork();
#endif
			//DoWork();
			//call the method from V-cache-manager
			QueryPerformanceCounter( &EndTimeThisRequestRun );
			duration.QuadPart = EndTimeThisRequestRun.QuadPart - StartServiceTime.QuadPart;
			float reqST = duration.QuadPart / frequency.QuadPart;

			////cout<<endl << endl<<"reqST :" <<reqST;
			//ST[idx%MAXA]=reqST;
			//idx++;


			////caculate the Q-time
			//if( (double) StartTimeThisRequestRun.QuadPart < L) delay=0;
			//else {
			//	delay = (double) StartTimeThisRequestRun.QuadPart - (StartTime.QuadPart + (frequency.QuadPart*startThisReq));
			//	delay = delay / frequency.QuadPart;
			//}

			//if (VERBOSE) printf("delay is %f.\n",delay);
			//
			
			
			lrt[ID].TN++;
			lrt[ID].TQT += delay;
			lrt[ID].TST += reqST;
			if (VERBOSE) {
				printf("we read the request %d",r->aRequest.key);
				cout<< endl <<endl <<endl;
			}
			
			//binary search
			float checkQT = 0;
			int num =0;
			for(int i=0 ; i<Num_of_thrds ; i++)
			{
				checkQT += lrt[i].TQT;	
				num += lrt[i].TN;

				
			}
			checkQT = checkQT / num;
			
			if (checkQT > (QThreshold) )
			{
				counter = 0;
				l->last_try_elt = l->num_of_populated_elt;
				Done = true;
				Max = Num_of_req;
				maxrepeat =true;
				first = false;

			}
			
			
				
			//after we finish with r
			l->freePopulatedElt(r);
			if (!ReleaseSemaphore(m_semaphore,    // handle to semaphore
								1,                // increase count by one
								NULL))            // not interested in previous count
			{
				printf("WorkerThreadConsumer %d failed to signal semaphore\n", ID);
				return -1;
			}
			
		}

		bool isf = l->isFinish();
		if (isf == true)
		{
			cout<< endl << "end of list, the result is not correct\n";
		}

		if (isf == true || EndTimeHasArrived() == true ) Done = true;

		

	}
	SetEvent(SyncEvent);

	return 0;
}

//it goes and read from the Trace File, send m request for the v-cache and do it until it fill the whole cache
//after that it put the A request in the linklist for later
void DoColdStart(WLGList *l, char* dataBuffer)
{
	OneRequest var1;
	WLGList *cacheL;
	cacheL=l;
	//Find the size of Cache; currently cache size is a constant - later, read from the configuration file
	//int size_of_each_request = cacheL->coldStartReq(&cacheL->);
	long long size_of_retrieved_req = 0;
	//Retrieve objects from the trace file until their total size equals (multiplier * Cache Size)
	while (cacheSize*ColdStartMultiplier > size_of_retrieved_req){
		l->coldStartReq(&var1);
		size_of_retrieved_req += var1.size;

#ifdef VCACHEMANAGER_RUN
		callVChash(var1, dataBuffer, dataBufferSize);   //We must call the appropriate command of the V cache manager using var1
#endif
	}

}





void callVChash(OneRequest var1, char* dataBuffer, int dataBufferSize)
{
	Vdt dzName, Key, Value;

	dzName.set_data( WLG_DZ[var1.zoneid].get_data() );
	dzName.set_size( WLG_DZ[var1.zoneid].get_size() );

	Key.set_data( (char*)&var1.key );
	Key.set_size( sizeof(var1.key) );

	Value.set_data( dataBuffer );
	Value.set_size( dataBufferSize );


	switch (var1.cmnd)
	{
	case Get:
		if( vcm->Get( &dzName, Key, &Value ) == 0 )
		{
			if( *(int*)Value.get_data() != var1.size )  
				cout<< "Error. Stored value does not match expected\n";
		}
		else
		{
			cout<< "Error. Could not get key\n";
		}
		break;
	case Insert:
		*(int*)dataBuffer = var1.size;
		Value.set_size( var1.size );
		if( vcm->Insert( &dzName, Key, Value ) == 0 )
		{

		}
		else
		{
			cout<< "Error. Could not Insert key\n";
		}
		break;
	case Delete:
		vcm->Delete( &dzName, Key );
		break;
	default :
		printf("Command %c is unknown.\n", var1.cmnd);
		break;
	}
}


int WLGmain( int argc, _TCHAR* argv[] )
{
	ConfigParametersV cp;
	ConfigParametersWLG cpw;
	memset( &cp, 0, sizeof(cp) );
	memset( &cpw, 0, sizeof(cpw) );


	ConfigurationLoader config;
	config.loadVConfigFile( "V_WLG.xml", cp );
	config.loadWLGConfigFile( "WLG_Params.xml", cpw );

	return WLGmain( argc, argv, cp, cpw );
}


//int _tmain(int argc, _TCHAR* argv[])
int WLGmain(int argc, _TCHAR* argv[], const ConfigParametersV& cp, const ConfigParametersWLG& cpw )
{
	//ConfigParametersV cp;
	//ConfigParametersTraceGen cpt;
	//ConfigurationLoader config;
	//config.loadVConfigFile( "V_WLG.xml", cp );
	//config.loadTraceConfigFile( "WLG_Params.xml", cpt );

#ifdef VCACHEMANAGER_RUN
	//*
	//TraceDirectory = (char*)(argv[1]);
	//TraceDirectory = (char*)cpt.filename.c_str();
    //NumPrefetchUnits = _ttoi(argv[2]);
	//NumPrefetchUnits = 10000;
	NumPrefetchUnits = cpw.NumPrefetchElements;
	//Min_Num_of_req = _ttoi(argv[3]);
	//Min_Num_of_req = 10;
	Min_Num_of_req = cpw.MinLambda;
	//Max_Num_of_req = _ttoi(argv[4]);
	//Max_Num_of_req = 15000;
	Max_Num_of_req = cpw.MaxLambda;
	//Granularity = _ttoi(argv[5]);
	//Granularity = 1000;		// Interarrival time = 1 sec
	Granularity = cpw.InterarrivalTime;
	//Distribution = (char*)(argv[6]);
	//Distribution= "uniform" ;
	Distribution = (char*)cpw.Distribution.c_str();
	//Num_of_thrds = _ttoi(argv[7]);
	Num_of_thrds = cpw.NumThreads;
	//Sim_Time = _ttoi(argv[8]);
	//Sim_Time = 1200000;		// 20 minutes
	Sim_Time = cpw.SimulationTime;

	
	if (Num_of_thrds > MAXTHREADS){
			printf("Number of threads %d may not exceed MAXTHREADS %d.\n", Num_of_thrds, MAXTHREADS);
			printf("Increase the constant MAXTHREADS to %d, recompile and run again.", Num_of_thrds+1);
			exit(1);
	}

	//*/
#else
	//*
	if (argc != 9)
	{
		printf("you should insert 8 input variable to be able to run the program but you insert %d variable", argc);
		cout << endl;
		cout << "please try again with correct variable" << std::endl;
		return -1;
	}
	else
	{
		
		TraceDirectory = (char*)(argv[1]);
	    NumPrefetchUnits = _ttoi(argv[2]);
		Min_Num_of_req = _ttoi(argv[3]);
		Max_Num_of_req = _ttoi(argv[4]);
		Granularity = _ttoi(argv[5]);
		Distribution = (char*)(argv[6]);
		//strcpy(Distribution,(char*)(argv[5]));
		Num_of_thrds = _ttoi(argv[7]);
		Sim_Time = _ttoi(argv[8]);

		//Error checking
		if(*Distribution == 'p' || *Distribution=='P') 
			*Distribution='p';
		else if(*Distribution == 'u' || *Distribution=='U') 
			*Distribution='u';
		else {
			printf("\n Error, input distribution must be letter p for Poisson or u for Uniform.  No other input is accepted.  Exiting.\n");
			exit(-1);
		}

		if (Num_of_thrds > MAXTHREADS){
			printf("Number of threads %d may not exceed MAXTHREADS %d.\n", Num_of_thrds, MAXTHREADS);
			printf("Increase the constant MAXTHREADS to %d, recompile and run again.", Num_of_thrds+1);
			exit(1);
		}
	}
	//*/
#endif

	time_t rawtime;

	//int QThresholdMultiplier = 10;
	int QThresholdMultiplier = cpw.TerminationThresholdMultiplier;
	QThreshold = QThresholdMultiplier * Granularity;
	//ColdStartMultiplier = 2;
	ColdStartMultiplier = cpw.ColdStartMultiplier;

	InitializeCriticalSection( &g_Stats_Update_CS );

	//cp.MemBDB.CacheSize.Bytes = cacheSize;

	/*
	if( cp.MemBDB.Enabled )
		cacheSize = cp.MemBDB.CacheSize.Bytes;
	else if( cp.DiskBDB.Enabled )
		cacheSize = cp.DiskBDB.CacheSize.Bytes;
	else
	{
		cacheSize = 0;
		cout<< "Warning. Starting experiment with 0 bytes of cache\n";
	}
	//*/
	cacheSize = cp.MemBDB.CacheSize.Bytes + cp.DiskBDB.CacheSize.Bytes;
	

	//TraceDirectory = "TF.txt";
	int fileCount = 0;
	int numFilesToProcess = cpw.NumIterations;
	string traceFilenames[4] = { 
		"C:\\jyap\\585\\Trace1M\\100M7Seed.txt",  
		"C:\\jyap\\585\\Trace1M\\10M101Seed.txt",
		"C:\\jyap\\585\\Trace1M\\Trace1M.2MGetSeed101", 
		"C:\\jyap\\585\\Trace1M\\Trace1M.1MGet" };
	
	// Load first file name from xml file
	traceFilenames[0] = cpw.filename;
	//traceFilenames[1] = cpw.filename2;
	TraceDirectory = (char*)traceFilenames[0].c_str();
	cout<< "\nNow processing trace file: " << traceFilenames[fileCount] << endl << endl;
		
	
	//test start for entry
	cout << "1- TraceDirectory : " << TraceDirectory << endl;
	cout << "2- NumPrefetchUnits : " << NumPrefetchUnits<< endl;
	cout << "3- Min_Num_of_req : " << Min_Num_of_req<< endl;
	cout << "4- Max_Num_of_req : " << Max_Num_of_req<< endl;
	cout << "5- Granularity : " << Granularity<< endl;
	cout << "6- Distribution : " << Distribution << endl;
	cout << "7- Num_of_thrds : " << Num_of_thrds << endl;
	cout << "8- Sim_Time (in msec) : " << Sim_Time << endl;

	
	
	QueryPerformanceFrequency( &frequency );
	frequency.QuadPart /= 1000;

	g_totalDelaySleepTime.QuadPart = 0;
	g_totalIntendedDelay = 0;
	g_numRequestsProcessed = 0;

	WLG_Filename = new char[WLG_NUMZONES];
	WLG_DZ = new Vdt[WLG_NUMZONES];
	//Initialize the DZ data structure
	for (int i = 0; i < WLG_NUMZONES; i++) {
		WLG_Filename[i] = 'A'+i;
		WLG_DZ[i].set_data(&WLG_Filename[i]);
		WLG_DZ[i].set_size(sizeof(WLG_Filename[i]));
	}

	

	vcm = new TCache();
	vcm->Initialize( cp );

	for( int i = 0; i < WLG_NUMZONES; i++ )
	{
		vcm->Create( &WLG_DZ[i] );
	}

	
	char dataBuffer[dataBufferSize];

	if (NumPrefetchUnits >= MAXA)
	{
		printf("Increase the constant MAXA in Common.h to exceed PrefetchUnits (%d), compile, and run the program again\n", NumPrefetchUnits);
		return -1;
	}
	
	Min = Min_Num_of_req;
	Max = Max_Num_of_req;
	Num_of_req = ((Min_Num_of_req + Max_Num_of_req )/2); 

	
	//at first we should insert "Number of Prefetch Units"(A) batches to the list
	//l = new WLGList(TraceDirectory, NumPrefetchUnits);
	l = new WLGList();

	//printf("Removed cold start to debug.... please put it back!");
	DoColdStart(l, dataBuffer);
	int ResultR = 0;
	float ResultAQT = 0;
	float ResultAST = 0;
	float ResultNumReq = 0;
	float TQT=0.0;
	int R = 0;
	int TST = 0;

	//vcm->ResetPerfCounterStats();

	//counter = 0;
	l->FirstGenerate();

	//l->FirstGenerate();
	while( fileCount < numFilesToProcess )
	{	
		g_traceEOFerror = false;

		Min = Min_Num_of_req;
		Max = Max_Num_of_req;
		Num_of_req = ((Min_Num_of_req + Max_Num_of_req )/2); 

		do
		{		
			
			//if (first != true) l->startAgain();
			//first=false;
			printf("Starting a new lambda.\n");

			if(!first)
			{
				while(l->getNumofLoaded() != 0)
				{
					//cout << l->getNumofLoaded()<<endl;
					ListElt *q = l->GetPopulatedElt();
					if( q != NULL )
						l->freePopulatedElt(q);
					else 
						l->reset();
				}
				l->p = NULL;
				l->t = NULL;

				if(!(Min == Min_Num_of_req && Max == Max_Num_of_req)) Num_of_req = ((Min + Max )/2);
				//counter = 0;

				l->FirstGenerate();
				//cout << "after" <<l->getNumofLoaded()<<endl;
			}
			
			////Flush all the free elements and populate them with trace elements
			//ListElt *p = l->getFreeElt();
			//while (p != NULL)
			//{
			//	l->GenerateOneRequest(p);
			//	if (VERBOSE) printf("Free Element was for object %d.\n",p->aRequest.key);
			//	l->AddToList(p);
			//	p = l->getFreeElt();
			//}
			
			cout<< endl<< "The max is:"<< Max<<endl;
			cout<<"The min is:"<<Min<<endl;
			cout<<"Lambda is: "<<Num_of_req<<" per "<<Granularity<<" msec."<<endl;

			for(int i =0; i<MAXTHREADS;i++)
			{
				lrt[i].TN=0;
				lrt[i].TQT =0;
				lrt[i].TST = 0;
			}

			//Reset the producer wait time so that it exits the semaphore
			ProdcuerWaitTime=INFINITE;

			m_semaphore= CreateSemaphore(
				NULL,   // no security attributes
				0,		// initial count
				MAXA,   // maximum count
				NULL);  // non named semaphore

			if ( !m_semaphore) 
			{
				printf("Error, failed to setup the synchronization mechanism(semaphore) Error in workload-genaretor.\n");
				return -1;
			}

			int NumConsumers = Num_of_thrds;
			int id = 1234;

			QueryPerformanceCounter( &SimulationEndTime );
			
			SimulationEndTime.QuadPart = SimulationEndTime.QuadPart + (frequency.QuadPart * Sim_Time ) ; //Denotes clock time when simulation must end
		 
		
			//creat 1 producer thread
			HANDLE producer_handle;
			producer_handle = CreateThread( NULL, 0, WorkerThreadProducer, NULL, 0, NULL);
			if ( producer_handle == NULL)
			{
				ExitProcess(id);
			}


			int IDs[MAXTHREADS];
			HANDLE Array_Of_Thread_Handles[MAXTHREADS];
			HANDLE SyncEvent = CreateEvent ( NULL , false , false , (LPCWSTR) "SyncEvent" );
				if ( !SyncEvent ) { 
				printf("Error, failed to setup the synchronization mechanism.\n");
				return -1; 
			}
		
			//creat NumConsumer Consumer threads
			for (int i = 0; i < NumConsumers; i++)
			{
				IDs[i] = i;
				Array_Of_Thread_Handles[i] = CreateThread( NULL, 0, WorkerThreadConsumer, &(IDs[i]), 0, NULL);
				if ( Array_Of_Thread_Handles[i] == NULL)
				{
					ExitProcess(IDs[i]);
				}
			}

			//at the end of program, we wait for all threads to finish their job and print the result and exit
			WaitForMultipleObjects( NumConsumers, Array_Of_Thread_Handles, TRUE, INFINITE);

			//Reset the producer wait time so that it exits the semaphore
			ProdcuerWaitTime=1000;

			if (!ReleaseSemaphore(m_semaphore,  // handle to semaphore
									1,                      // increase count by one
									NULL))            // not interested in previous count
			{
				printf("Main failed to signal semaphore\n");
				return -1;
			}
			


			//Wait for the producer thread to ensure it has finished
			WaitForSingleObject(producer_handle, INFINITE);

			//Close the semaphore
			CloseHandle(m_semaphore);

			time( &rawtime );

		
			R=0;
			TQT=0;
			TST=0;
			
			for(int i=0;i<Num_of_thrds;i++)
			{
				R += lrt[i].TN;
				TQT += lrt[i].TQT;
				TST += lrt[i].TST;
			}
			
			
			if(TQT/R <=(QThreshold))
			{				
				l->last_try_elt = l->num_of_populated_elt;
				ResultR=R;
				ResultAQT = (TQT/R);
				ResultAST = (float(TST)/R);
				ResultNumReq = Num_of_req;
				Min = Num_of_req;
				minrepeat = true;
				first = false;
			}

			cout<< "Total number of elements read from the trace: " << l->TraceEltsProcessed() << endl;
			printf("The Total Number of Request processed with this lambda is: %d (for %d msec of simulation time)", R,Sim_Time);
			cout << endl;
			printf("Observed Average-Service-Time is: %f", (float(TST)/R));
			cout << endl;
			printf("Observed Average Q-Time is: %f ", (TQT/R));
			cout << endl;
			printf("Observed Average Response-Time is: %f ", (float(TST)/R)+(TQT/R));
			cout << endl;
			cout<< "Total measured delay sleep time for this lambda: " << g_totalDelaySleepTime.QuadPart / double(frequency.QuadPart) << " msec\n";
			cout<< "Total intended delay sleep time for this lambda: " << g_totalIntendedDelay << " msec\n";
			cout<< "Total number of elements processed: " << g_numRequestsProcessed << endl;
			cout << endl;
			cout<< "The current time is: " << ctime( &rawtime ) << endl;
			cout << endl;

			if( g_currentLambdaSuccess )
			{
				g_lastSuccessfulIntendedDelay = g_totalIntendedDelay;
				g_lastSuccessfulNumRequestsProcessed = g_numRequestsProcessed;
				g_lastSuccessfulTotalDelaySleepTime = g_totalDelaySleepTime;

				g_currentLambdaSuccess = false;
			}
			
			// Reset sleep time statistics for each try of lambda
			g_totalDelaySleepTime.QuadPart = 0;
			g_totalIntendedDelay = 0;
			g_numRequestsProcessed = 0;

			if (Max - Min == 1)
			{
				if(minrepeat == false)
				{
					printf("your min and max number of request are so large. it is not a successfull expriment, decrease the min and run it again\n");
					break;
				}
				if(maxrepeat == false)
				{
					printf("your min and max number of request are so small. it is not a successfull expriment, increase the max and run it again\n");
					break;
				}
				break;

			}

		}while (maxrepeat || minrepeat);

		cout << endl;
		printf("\n\n------------------------\nFinal Results:-\nInterarrival time is %d msec.", Granularity);
		cout << endl;
		printf("Ideal lambda is %f requests per interarrival time.",ResultNumReq);
		cout << endl;
		cout << endl;
		printf("The Total Number of Request processed with this lambda is: %d (for %d msec of simulation time)", ResultR,Sim_Time);
		cout << endl;
		printf("Observed Average-Service-Time is: %f", ResultAST);
		cout << endl;
		printf("Observed Average Q-Time is: %f ", ResultAQT);
		cout << endl;
		printf("Observed Average Response-Time is: %f ", ResultAQT+ResultAST);
		cout << endl;
		cout<< "The current time is: " << ctime( &rawtime ) << endl;
		cout<< "Total measured delay sleep time for this lambda: " << g_lastSuccessfulTotalDelaySleepTime.QuadPart / double(frequency.QuadPart) << " msec\n";
		cout<< "Total intended delay sleep time for this lambda: " << g_lastSuccessfulIntendedDelay << " msec\n";
		cout<< "Total number of elements processed: " << g_lastSuccessfulNumRequestsProcessed << endl;
		cout << endl;
		cout << endl;

		ofstream fout;
		fout.open( "BinarySearchResults.csv", ofstream::app );

		if( g_traceEOFerror )
		{
			fout<< "CacheSize: " << cacheSize << " bytes;  NumThreads " << cpw.NumThreads << endl;
			fout<< "**ERROR** Could not finish experiment because trace was not large enough\n";
		}
		else
		{
			/*
			fout<< "CacheSize: " << cp.MemBDB.CacheSize.Bytes << " bytes;  NumThreads " << cpt.NumThreads << endl;
			fout<< "TraceFile: " << traceFilenames[fileCount] << endl;
			fout<< "Ideal lambda: " << ResultNumReq << endl;
			fout<< "The Total Number of Request processed with this lambda is: " << ResultR << "(for " << Sim_Time << " msec of simulation time)\n";
			fout<< "Observed Average-Service-Time is: " << ResultAST << endl;
			fout<< "Observed Average Q-Time is: " << ResultAQT << endl;
			fout<< "Observed Average Response-Time is: " << ResultAQT+ResultAST << endl;		
			//*/

			int algorithmID = 0;
			if( cp.MemBDB.Enabled )
				algorithmID = cp.MemBDB.ReplacementTechniqueID;

			fout<< "\"TraceFile:" << traceFilenames[fileCount] << "\"\n";
			fout<< ctime( &rawtime ) << endl;
			fout<< "SimTime,ColdStart,InterarrivalTime,CacheSize,NumMemPartitions,NumThreads,Threshold,Distribution,AlgorithmID\n";
			fout<< Sim_Time << ","
				<< ColdStartMultiplier << ","
				<< Granularity << ","
				<< cacheSize << ","
				<< cp.MemBDB.DatabaseConfigs.Partitions << ","
				<< cpw.NumThreads << ","
				<< QThresholdMultiplier << ","
				<< Distribution << ","
				<< algorithmID << endl;


			fout<< "IdealLambda,Average-Q,CacheHit-SW,ByteHit-SW,NumQ,TotalBytes,TotalSleepTime\n";
			fout<< ResultNumReq << "," 
				<< ResultAQT << "," 
				<< vcm->GetStatisticsManager()->GetCacheHitRate() << ","
				<< vcm->GetStatisticsManager()->GetByteHitRate() << ","
				<< vcm->GetStatisticsManager()->GetNumQ() << ","
				<< vcm->GetStatisticsManager()->GetTotalBytes() << ","
				<< g_lastSuccessfulTotalDelaySleepTime.QuadPart / double(frequency.QuadPart)  << endl;


			/* // Dump the contents of the cache to a file
			ofstream foutQDB;
			string qdbfilename = "QDBContents-" + cpw.OutputFileName + ".csv";
			foutQDB.open( qdbfilename.c_str(), ofstream::out );
			foutQDB<< setprecision(20);
			vcm->DumpCache( foutQDB );
			foutQDB.close();
			//*/

		}
		fout<< endl << endl;

		fout.flush();
		fout.close();

		fileCount++;
		if( fileCount < numFilesToProcess )
		{
			// TODO: fix memory leak with list, can't delete or it crashes
			//delete l;
			TraceDirectory = (char*)traceFilenames[fileCount].c_str();
			l = new WLGList();
			cout<< "\nNow processing trace file: " << traceFilenames[fileCount] << endl << endl;
		}
	}

	// Shutdown TCache
	vcm->Shutdown();


	return -1;	
}
