// HashTable.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "HashIdx.h"
using namespace std;

int Create(char *datazone_name,Db* &dbptr)
{

	Db *dz_dbp = NULL;
	//dz_dbp=new Db(NULL,0);// =new Db( NULL, 0 );
	try {
		dz_dbp = new Db(NULL, 0);

		int open_flags = DB_CREATE ;              // Allow database creation
                    
        dz_dbp->open(NULL,       // Txn pointer
                  datazone_name,   // File name
                  NULL,       // Logical db name
                  DB_HASH,   // Database type (using hash)
                  open_flags,  // Open flags
                  0);         // File mode. Using defaults

		dbptr=dz_dbp;		
		//dz_dbp->close(0);
				    
	} catch(DbException &e) {
        cout << e.what() << endl;
        if (dz_dbp!=NULL)
			dz_dbp->close(0);
		//e.get_errno()
		return (-1);
    } catch(exception &e) {
        cout << e.what() << endl;
        return (-1);
    }
	return 0;
}

int SimpleTest(HashIdx *HI)
{
	Db *dbptr;
	int ret;
	char* data = new char[100];
	char* key = new char[100];

	cout<<"Inserting\n\n";
	for (int i= 0; i < 10; i++)
	{
		sprintf(key, "dzname%d", i);
		sprintf(data, "Dzname%d", i);
		ret=Create(data,dbptr);
		if(ret)
		{
			cout<<"Error creating database"<<endl;
			return(-1);
		}
		std::cout<<"Dbptr is "<<dbptr<<std::endl;
		int ret=HI->Put(key, strlen(key)+1, dbptr);
		//if(ret==0) printf("Put %d successfull\n",i);
		//sprintf_s(data, "Key%d", i);
	}

	cout<<"\nGet\n\n";
	Db *DbHandle[10];
	for (int i = 0; i < 10; i++)
	{
		sprintf(key, "dzname%d", i);
		DbHandle[i] = HI->Get(key, strlen(key)+1);
		cout<<"Dbptr is "<<DbHandle[i]<<endl;
	}

	Db *handle;
	cout<<"\nDelete\n\n";
	for (int i = 0; i < 12; i++)
	{
		//sprintf(key, "dzname%d", i);
		//ret=HI->Delete(key,strlen(key)+1);
		ret = HI->DeleteOneKeyAndReturnDB(handle);
		if (ret) 
		{
			printf("Error, Delete failed with %d.\n", i);
		}
		else
		{
			cout<<"Deleted Dbptr is "<<handle<<endl;
			if (handle!=NULL)
				handle->close(0);
			else
				cout<<"Handle is NULL"<<endl;
		}
	}
	return 0;
}


int _tmain(int argc, _TCHAR* argv[])
{
	HashIdx *HI = new HashIdx();
	int ret=SimpleTest(HI);
	system("pause");
	return 0;
}

