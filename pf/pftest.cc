#include <iostream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;


// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}

int PFTest_1(PF_Manager *pf)
{
    // Functions Tested:
    // 1. CreateFile
    cout << "****In PF Test Case 1****" << endl;

    RC rc;
    string fileName = "test";

    // Create a file named "test"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        return -1;
    }

    // Create "test" again, should fail
    rc = pf->CreateFile(fileName.c_str());
    assert(rc != success);

    return 0;
}

int PFTest_2(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. AppendPage
    // 3. GetNumberOfPages
    // 4. WritePage
    // 5. ReadPage
    // 6. CloseFile
    // 7. DestroyFile
    cout << "****In PF Test Case 2****" << endl;

    RC rc;
    string fileName = "test";

    // Open the file "test"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    
    // Append the first page
    // Write ASCII characters from 32 to 125 (inclusive)
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);
   
    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)1);

    // Update the first page
    // Write ASCII characters from 32 to 41 (inclusive)
    data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.WritePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);
 
    // Close the file "test"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);
    
    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 2 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        return -1;
    }
}

void PFTest_3(PF_Manager *pf)
{
	int rc;

	rc = pf->CreateFile("test");

	cout<<"Return code on creation of a new file : "<<rc<<endl;

	rc = pf->DestroyFile("test1");

	cout<<"Return code on destruction of non-existent file : "<<rc<<endl;

	rc = pf->DestroyFile("test");

	cout<<"Return code on destruction of existing file :"<<rc<<endl;

	cout<<"Creation and Destruction have been solved!"<<endl;

	PF_FileHandle fileHandle;

	pf->CreateFile("test");

	rc = pf->OpenFile("test1",fileHandle);

	cout<<"Return code on trying to open a non-existent file :"<<rc<<endl;

	rc = pf->OpenFile("test",fileHandle);

	cout<<"Return code on trying to open an existent file :"<<rc<<endl;

	PF_FileHandle fileHandle1;

	rc = pf->OpenFile("test",fileHandle1);

	cout<<"Return code on multiple read : "<<rc<<endl;
	
	rc = pf->CloseFile(fileHandle1);

	cout<<"Return code on closing a file that exists :"<<rc<<endl;

	rc = pf->CloseFile(fileHandle1);

	cout<<"Return code on closing a file that does not exist : "<<rc<<endl;

	rc = fileHandle.GetNumberOfPages();

	cout<<"Number of pages in an empty file : "<<rc<<endl;

	char data[4096];

	for(int i=0;i<4096;i++)
		data[i] = i%10 + 48;

	rc = fileHandle.WritePage(1,data);

	cout<<"Return code on writing to a page that does not exist!"<<rc<<endl;

	rc = fileHandle.WritePage(0,data);

	cout<<"Return code on writing to page 0 that does not exist!"<<rc<<endl;

	rc = fileHandle.AppendPage(data);

	cout<<"Return code on appending a page and writing data to it :"<<rc<<endl;

	char data_cpy[4096];

	rc = fileHandle.ReadPage(1,data_cpy);

	cout<<"Return code on reading from a page that does not exist! : "<<rc<<endl;

	cout<<"data_cpy holds this after reading from a non-existent page : "<<endl;

	for(int i=0;i<4096;i++)
	{
		cout<<data_cpy[i];
	}

	cout<<endl;

	rc = fileHandle.ReadPage(0,data_cpy);

	cout<<"Return code after reading from a page that exists : "<<rc<<endl;

	for(int i=0;i<4096;i++)
	{
		cout<<data_cpy[i];
	}

	cout<<endl;

	rc = fileHandle.GetNumberOfPages();

	cout<<"Number of pages in a file with contents on page 0 : "<<rc<<endl;

	char data1[4096];

	for(int i=0;i<4096;i++)
	{
		data1[i] = i%9+48;
	}

	fileHandle.AppendPage(data1);

	char data1_cpy[4096];
	
	fileHandle.ReadPage(1,data1_cpy);

	for(int i=0;i<4096;i++)
	{
		cout<<data1_cpy[i];
	}

	cout<<endl;

	rc = fileHandle.GetNumberOfPages();

	cout<<"Number of pages in a file that has contents in pages 0 and 1 : "<<rc<<endl;

	rc = pf->CloseFile(fileHandle);

	cout<<"Return code on closing existent file that is open : "<<rc<<endl;

	rc = pf->OpenFile("test",fileHandle1);

	rc = fileHandle1.GetNumberOfPages();

	cout<<"Number of pages in a file that has contents in pages 0 and 1 from a different file handle : "<<rc<<endl;

	rc = pf->CloseFile(fileHandle1);
}

void PFTest_4(PF_Manager *pf)
{
	int rc;
	
	rc = pf->CreateFile("test");

	PF_FileHandle fileHandle;

	char data[4096];

	rc = fileHandle.ReadPage(0,data);

	cout<<"Return code on reading from a file handle that does not hold a file :"<<rc<<endl;

	rc = fileHandle.WritePage(0,data);

	cout<<"Return code on writing to a file handle that does not hold a file :"<<rc<<endl;

	rc = fileHandle.AppendPage(data);

	cout<<"Return code on Append :"<<rc<<endl;

	unsigned int rc1 = fileHandle.GetNumberOfPages();

	cout<<"Return code on GetNumberOfPages :"<<rc1<<endl;

	rc = pf->DestroyFile("test");
}

void PFTest_5(PF_Manager *pf)
{
	char data[4096];

	for(int i=0;i<4096;i++)
	{
		data[i] = i%10+48;
	}

	int rc = pf->CreateFile("test");

	assert(rc==0);

	PF_FileHandle fileHandle;

	rc = pf->OpenFile("test",fileHandle);

	assert(rc==0);

	rc = fileHandle.AppendPage(data);

	assert(rc==0);

	for(int i=0;i<4096;i++)
	{
		data[i]=i%26+65;
	}

	rc = fileHandle.AppendPage(data);

	assert(rc==0);

	for(int i=0;i<4096;i++)
	{
		data[i] = i%26+97;
	}

	rc = fileHandle.AppendPage(data);

	assert(rc==0);

	unsigned int nop = fileHandle.GetNumberOfPages();

	assert(nop==3);

	for(int i=0;i<4096;i++)
	{
		data[i] = i%9+48;
	}

	rc = fileHandle.WritePage(1,data);

	assert(rc==0);

	assert(nop==3);

	rc = pf->CloseFile(fileHandle);

	assert(rc==0); 
}

void PFTest_6(PF_Manager *pf)
{
	int rc;

	rc = pf->CreateFile("hello.bin");

	PF_FileHandle fileHandle;

	rc = pf->OpenFile("hello.bin",fileHandle);

	void *data = malloc(PF_PAGE_SIZE);		

	cout<<"data info : "<<endl;

	for(int i = 0; i < 4096; i=i+4)
    	{
        	memcpy((char *)data + i, &i, sizeof(int));
    	}

	cout<<"data size : "<<strlen((char*)data)<<endl;

	cout<<endl<<endl;

	rc = fileHandle.AppendPage(data);

	void *buffer = malloc(PF_PAGE_SIZE);

	rc = fileHandle.ReadPage(0,buffer);

	cout<<"buffer info :"<<endl;

	int x;

	for(int i=0;i<4096;i=i+4)
	{
		memcpy(&x,(char*)data+i,sizeof(int));

		cout<<x<<"\t";
	}
	

	cout<<endl;

	free(data);
	free(buffer);

	rc = pf->CloseFile(fileHandle);


	rc = pf->DestroyFile("hello.bin");
}


int main()
{
    PF_Manager *pf = PF_Manager::Instance();
    remove("test");
   
    PFTest_1(pf); 
    PFTest_2(pf); 
 
	//PFTest_3(pf);
	//PFTest_4(pf);

	//PFTest_5(pf); 
  
	PFTest_6(pf);
//	PFTest_7(pf);
    return 0;
}
