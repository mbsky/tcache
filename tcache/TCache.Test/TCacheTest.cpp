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
#include <stdio.h>
#include "TCache.h"
#include "ConfigurationLoader.h"
#include "Common.h"
#include <db_cxx.h>


// Define the name and path of the configuration file 
#define CONFIG_FILE "V.xml"

const int size=200;

// Define the number and names of the datazones to be created
#define NUM_DATAZONE 2 
char create_datazone[][size]={"Superhero","Homosapien"};

// Define the seperator for the semi-strcutred record test
#define SEPERATOR ","

// Should the output be verbose?
#define VERBOSE true
//#define VERBOSE false

struct superhero_structure {
	int num_enemies; //Number of Enemies
	int num_movies; //Number of movies made(as per imdb search with superhero name)
	int year_of_origin; //Year of origin
};

void draw_line() {
	printf("\n");
	for(int i=0;i<80;i++)
		printf("-");
	printf("\n\n");
}

// This test 
// a)Inserts key value pairs into the datazones
// b)Retrieves the key value pairs from the datazones
// c)Delete the key value pairs from the datazones
// d)Retrieves the key value pair from the datazones which should fail.  
int keyValueTest(TCache *tc)
{
	if(VERBOSE)
		printf("*~*-~- Performing the key value test -~-*~*\n\n\n");

	const int key_index=0;
	const int value_index=1;
	const int datazone_index=2;
	
	const int num_fields=3;
	const int num_objects=15;

	// Format: 
	// "key", "value" ,"datazone" 
	// i.e The first field is the key, the second one is the value and the third one is the datazone
	char record[num_objects][num_fields][size] ={
		"Superman"		,"It's a bird, It's a plane, It's Superman"						,"Superhero",
		"Spiderman"	,"With great power comes great responsibilty"					,"Superhero",
		"The Thing"	,"It's clobberin' time!"											,"Superhero",
		"Batman"		,"Criminals are a superstitious and cowardly lot"				,"Superhero",
		"Wonderwoman"	,"I am Diana, princess of the Amazons"							,"Superhero",
		"Mr Incredible","The world always manages to get back in jeopardy again"		,"Superhero",
		// Same key value pair in a different datazone, the datazone acts like a namespace
		"Mr Incredible","The world always manages to get back in jeopardy again"		,"Homosapien",
		"Edward Blake" ,"Mother forgive me"												,"Superhero",
		// Same key value pair in a different datazone, the datazone acts like a namespace
		"Edward Blake" ,"Mother forgive me"												,"Homosapien",

		"Prof Shahram","Knows all"														,"Homosapien",
		"Igor"			,"Complains all"													,"Homosapien",
		"Elham"			,"Questions all"													,"Homosapien",
		"Jason"			,"Gets all"														,"Homosapien",
		"Jorge"			,"Terminates all"													,"Homosapien",
		"Showvick"		,"Saves all"														,"Homosapien"
	};
	
	// Vdt is the data structure to store opaque objects
	Vdt vdt_datazone[num_objects],vdt_key[num_objects],vdt_value[num_objects];
	char *get_buffer= new char[size];
	Vdt vdt_get_value(get_buffer,size);

	int ret=0,i;
	
	//Create the vdts
	for(i=0;i<num_objects;i++)
	{
		//Convert the cstring to VDT
		vdt_key[i]= TCUtility::cstringToVdt(record[i][key_index]);
		vdt_value[i]= TCUtility::cstringToVdt(record[i][value_index]);
		vdt_datazone[i]= TCUtility::cstringToVdt(record[i][datazone_index]);
	}

	if(VERBOSE)
		printf("Inserting the key value pairs into the datazones\n\n");

	// Insert the key value pair to the datazones
	for(i=0;i<num_objects;i++)
	{
		//Perform the insert
		ret=tc->Insert(&vdt_datazone[i],vdt_key[i],vdt_value[i]);
		if(ret!=0) {
			printf("Failed to insert record %s\n",db_strerror(ret));
			return -1;
		}
	}

	if(VERBOSE)
		printf("Retrieving the key value pairs from the datazones\n");
	// Get the key value pair to verify whether it is inserted 
	for(i=0;i<num_objects;i++)
	{
		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(size);
		//Perform the get
		ret=tc->Get(&vdt_datazone[i],vdt_key[i],&vdt_get_value);
		if(ret!=0) {
			printf("Failed to get record %s\n",db_strerror(ret));
			return -1;
		}

		// Check if the retrieved value is what we stored
		if(strncmp(get_buffer,record[i][value_index],strlen(record[i][value_index]))!=0)
		{
			printf("The Retrieved value does not match the inserted value \n");
			return -1;
		}
		if(VERBOSE)
			printf("%s %s \n",vdt_key[i].get_data(),get_buffer);

	}

	if(VERBOSE)
		printf("\nDeleting the key value pair from the datazone\n\n");


	// Delete the records 
	for(i=0;i<num_objects;i++)
	{
		//Perform the delete
		ret= tc->Delete(&vdt_datazone[i],vdt_key[i]);
		if(ret!=0) {
			printf("Failed to delete record %s\n",db_strerror(ret));
			return -1;
		}
	}

	// Get the records to verify if it is deleted
	for(i=0;i<num_objects;i++)
	{
		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(size);
		//Perform the get
		ret=tc->Get(&vdt_datazone[i],vdt_key[i],&vdt_get_value);
		if(ret==0) {
			printf("Failed to delete record it still exist in the system\n");
			return -1;
		}
	}
	
	delete [] get_buffer;
	return 0;
}


// This test demonstrates the opaque nature of the VDT datastructure. It 
// a)Inserts pairs of key and structured record for the value into the datazones
//	  The structured record consists of Number of Enemies,Number of movies made(as per imdb search with superhero name),Year of origin
// b)Retrieves the key and structured record pair from the datazones

int structuredRecordTest(TCache *tc)
{
	
	if(VERBOSE)
		printf("*~*-~- Performing the structued record test -~-*~*\n\n\n");

	const int num_structure_fields=3;
	const int num_objects=5;

	const int datasize=sizeof(superhero_structure);
	// Format: 
	// "key", = "Superhero name"	
	char record[num_objects][size] ={
		"Superman",
		"Batman",
		"Wonderwoman",
		"Green Lantern",
		"Flash"
	};

	// Foramt:
	// Number of Enemies,Number of movies made(as per imdb search with superhero name),Year of origin
	int superhero_structure_data[num_objects][num_structure_fields]={
		35,	4,	1938,
		34,	9,	1939,
		29,	5,	1941,
		6,	2,	1940,
		9,	1,	1940
	};


	// Vdt is the data structure to store opaque objects
	Vdt vdt_key,vdt_get_value,vdt_insert_value;

	// Initialize the datazone vdt 
	Vdt vdt_datazone= TCUtility::cstringToVdt(create_datazone[0]);

	superhero_structure superhero;

	char *get_buffer= new char[datasize];
	
	int ret=0,i;

	if(VERBOSE)
		printf("Inserting the records into the datazones\n\n");
	
	//Insert the records merging with the seperator 
	for(i=0;i<num_objects;i++)
	{
		// Initialize the VDT
		vdt_key= TCUtility::cstringToVdt(record[i]);
		
		//initialize the structure as per the data 
		superhero.num_enemies=superhero_structure_data[i][0];
		superhero.num_movies=superhero_structure_data[i][1];
		superhero.year_of_origin=superhero_structure_data[i][2];

		//Since the vdt is opaque, we can directly initialize it with the structure 
		//typecasting it to char * and specifying the size. 
		vdt_insert_value.set_data((char *)&superhero);
		vdt_insert_value.set_size(sizeof(superhero));
		
		//Perform the insert
		ret=tc->Insert(&vdt_datazone,vdt_key,vdt_insert_value);
		if(ret!=0) {
			printf("Failed to insert record %s\n",db_strerror(ret));
			return -1;
		}
	}

	if(VERBOSE)
		printf("Retrieving the records from the datazones\n");

	// Get the key value pair to verify whether it is inserted 
	for(i=0;i<num_objects;i++)
	{
		// Initialize the VDT
		vdt_key= TCUtility::cstringToVdt(record[i]);

		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(datasize);

		//Perform the get
		ret=tc->Get(&vdt_datazone,vdt_key,&vdt_get_value);
		if(ret!=0) {
			printf("Failed to get record %d %s\n",i,db_strerror(ret));
			return -1;
		}

		//Type case the retrieved value into superhero structure
		superhero_structure get_superhero= *(superhero_structure *) get_buffer;

		// Check if the retrieved value is what we stored
		if(get_superhero.num_enemies != superhero_structure_data[i][0])
		{
			printf("The Retrieved number of enemies does not match the inserted value \n");
			return -1;
		}

		if(get_superhero.num_movies != superhero_structure_data[i][1])
		{
			printf("The Retrieved number of movies does not match the inserted value \n");
			return -1;
		}

		if(get_superhero.year_of_origin != superhero_structure_data[i][2])
		{
			printf("The Retrieved year of origin does not match the inserted value \n");
			return -1;
		}

		if(VERBOSE)
			printf("%s %d %d %d \n",vdt_key.get_data(),get_superhero.num_enemies,get_superhero.num_movies,get_superhero.year_of_origin );

	}

	if(VERBOSE)
			printf("\n");
	delete [] get_buffer;
	return 0;
}


// This test 
// a)Inserts pairs of key and comma seperated value (any seperator can be used, defined in SEPERATOR macro) into the datazones
// b)Retrieves the key and comma seperated value from the datazones
int semiStructuredRecordTest(TCache *tc)
{
	
	const int num_fields=5;
	const int num_objects=9;

	const int key_index=0;

	const int datasize=size*(num_fields-1);

	if(VERBOSE)
		printf("*~*-~- Performing the semi structued record test -~-*~*\n\n\n");
		
	// Format: 
	// "Superhero name", "first name", "last name", "email", "year of origin"
	// "key", = "Superhero name"	
	// "value" = "first name", "last name", "email", "year of origin" Seperated by seperator

	char record[num_objects][num_fields][size] ={
		"Superman"		,"Clark"	,"Kent"		,"kent@DailyPlanet.com"	,"1938",
		"Batman"		,"Bruce"	,"Wayne"	,"bruce@gotham.org"		,"1939",
		"Robin"			,"Dick"		,"Grayson"	,"grayson@gotham.org"		,"1940",
		"CatGirl"		,"Selina"	,"Kyle"		,"meow@gotham.org"			,"1986",
		"Wonderwoman"	,"Diana"	,"Prince"	,"wonderwoman@legion.org"	,"1941",
		"Spiderman"	,"Peter"	,"Parker"	,"parker@nyc.org"			,"1962",
		"Flash"			,"Jay"		,"Garrick"	,"garrick@keystone.org"	,"1940",
		"Thor"			,"Donald"	,"Blake"	,"blake@odin.com"			,"1962",
		"Huntress"		,"Helena"	,"Wayne"	,"helena@gotham.org"		,"1947"
	};
	

	// Vdt is the data structure to store opaque objects
	Vdt vdt_key,vdt_get_value,vdt_insert_value;
	
	// Initialize the datazone vdt 
	Vdt vdt_datazone= TCUtility::cstringToVdt(create_datazone[0]);

	char *get_buffer= new char[datasize];
	char *insert_buffer= new char[datasize];
	
	int ret=0,i;
	
	if(VERBOSE)
		printf("Inserting the records into the datazones\n\n");

	//Insert the records merging with the seperator 
	for(i=0;i<num_objects;i++)
	{
		// Initialize the VDT
		vdt_key= TCUtility::cstringToVdt(record[i][key_index]);

		memset(insert_buffer,0,datasize);

		// Put the record data into the insert buffer with the seperator
		sprintf(insert_buffer,"%s" SEPERATOR "%s" SEPERATOR "%s" SEPERATOR "%d",
								record[i][1],record[i][2],record[i][3] ,atoi(record[i][4]));
		
		vdt_insert_value.set_data(insert_buffer);
		vdt_insert_value.set_size(strlen(insert_buffer));
		
		//Perform the insert
		ret=tc->Insert(&vdt_datazone,vdt_key,vdt_insert_value);
		if(ret!=0) {
			printf("Failed to insert record %s\n",db_strerror(ret));
			return -1;
		}
	}

	if(VERBOSE)
		printf("Retrieving the records from the datazones\n");

	// Get the key value pair to verify whether it is inserted 
	for(i=0;i<num_objects;i++)
	{
		// Initialize the VDT
		vdt_key= TCUtility::cstringToVdt(record[i][key_index]);

		vdt_get_value.set_data(get_buffer);
		vdt_get_value.set_size(datasize);

		//Perform the get
		ret=tc->Get(&vdt_datazone,vdt_key,&vdt_get_value);
		if(ret!=0) {
			printf("Failed to get record %d %s\n",i,db_strerror(ret));
			return -1;
		}

		// Construct the tokenized fields "first name", "last name", "email", "year of origin"
		char first_name[size], last_name[size], email[size],*p;
		int year_of_origin;

		get_buffer[vdt_get_value.get_size()]='\0';
				
		p= strtok(get_buffer,SEPERATOR);
		strcpy(first_name,p);
		
		p= strtok(NULL,SEPERATOR);
		strcpy(last_name,p);
		
		p= strtok(NULL,SEPERATOR);
		strcpy(email, p);

		p= strtok(NULL,SEPERATOR);
		year_of_origin= atoi(p);
		
		// Check if the retrieved value is what we stored
		if(strncmp(first_name,record[i][1], strlen(record[i][1]))!=0)
		{
			printf("The Retrieved first name does not match the inserted value \n");
			return -1;
		}

		if(strncmp(last_name,record[i][2], strlen(record[i][2]))!=0)
		{
			cout<<last_name<<" "<<record[i][2];
			printf("The Retrieved last name does not match the inserted value \n");
			return -1;
		}

		if(strncmp(email,record[i][3], strlen(record[i][3]))!=0)
		{
			printf("The Retrieved email does not match the inserted value \n");
			return -1;
		}

		if(year_of_origin != atoi(record[i][4]))
		{
			printf("The Retrieved year of origin does not match the inserted value \n");
			return -1;
		}

		if(VERBOSE)
			printf("%s %s %s %s %d\n",vdt_key.get_data(),first_name,last_name,email,year_of_origin);


	}

	if(VERBOSE)
		printf("\n");
	delete [] get_buffer;
	delete [] insert_buffer;
	return 0;
}



//int _tmain(int argc, _TCHAR* argv[])
int TCacheSimpleTest()
{
	int ret=0,i;

	// Create a TCache Object
	TCache tc;
	
	// Load the configuration parameters from the XML file. 
	ConfigParametersV cp;
	ConfigurationLoader config;
	config.loadVConfigFile(CONFIG_FILE , cp );

	// Initialize the instance of the trojan cache with configuration parameters
	ret=tc.Initialize(cp) ;
	if(ret!=0) {
		printf("Failed to initialize the system. Aborting the test %s\n",db_strerror(ret));
		return -1;
	}

		// Vdt is the data structure to store opaque objects
	Vdt vdt_datazone;
		
	// Create the datazones
	for(i=0;i<NUM_DATAZONE;i++)
	{
		// Initialize the VDT
		vdt_datazone= TCUtility::cstringToVdt(create_datazone[i]);

		// Create the datazone
		ret=tc.Create(&vdt_datazone);
		if(ret!=0 &&ret!=-1) {
			printf("Failed to create datazone %s\n",db_strerror(ret));
			return -1;
		}
	}

	if (VERBOSE)
		draw_line();

	ret= keyValueTest(&tc);
	if(ret==0) 
		printf("Key Value Test passed \n");
	else 
		printf("Failed the KeyValue Test \n");

	if (VERBOSE)
		draw_line();

	ret= structuredRecordTest(&tc);
	if(ret==0) 
		printf("Structured Record Test passed \n");
	else 
		printf("Failed the Structured Record Test \n");

	if (VERBOSE)
		draw_line();

	ret= semiStructuredRecordTest(&tc);
	if(ret==0) 
		printf("Semi-structured Record Test passed \n");
	else 
		printf("Failed the Semi-structured Record Test \n");

	if (VERBOSE)
		draw_line();

	system("pause");
	// Shutdown the system
	tc.Shutdown();
	return 0;
	
}
