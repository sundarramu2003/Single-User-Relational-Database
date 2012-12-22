#include "pf.h"
#include<fstream>
#include<iostream>
using namespace std;

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{	
	fstream file;

	file.open(fileName,ios::in|ios::binary);

	if(file.is_open())
	{
		file.close();
		return -1;
	}

	file.open(fileName,ios::out|ios::binary);
	file.close();

	return 0;
			
}


RC PF_Manager::DestroyFile(const char *fileName)
{
	if(remove(fileName)==0)
		return 0;	
    return -1;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
	if(fileHandle.IsFree()==0)
		return fileHandle.SetHandle(fileName);
	else
		return -1;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
    return fileHandle.DropHandle();
}


PF_FileHandle::PF_FileHandle()
{
}
 

PF_FileHandle::~PF_FileHandle()
{
	if(file.is_open())
		file.close();
}

RC PF_FileHandle::SetHandle(const char *fileName)
{
	file.open(fileName,ios::in|ios::out|ios::binary);

	if(file.is_open())
		return 0;

	return -1;
}

RC PF_FileHandle::DropHandle()
{
	if(file.is_open())
	{
		file.close();
		return 0;
	}

	return -1;
}

RC PF_FileHandle::IsFree()
{
	if(file.is_open())
		return -1;
	else
		return 0;
}



RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
	if(IsFree()==0)
		return -1;

	if(GetNumberOfPages()<=pageNum)
		return -1;


	file.seekg(pageNum*PF_PAGE_SIZE,ios::beg);

	file.read((char*)data,PF_PAGE_SIZE);
		
	return 0;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
	if(IsFree()==0)
		return -1;

	if(GetNumberOfPages()<=pageNum)
		return -1;
	

	else
	{
		file.seekg(pageNum*PF_PAGE_SIZE,ios::beg);	
		file.write((char*)data,PF_PAGE_SIZE);
	}
	return 0;
		
}


RC PF_FileHandle::AppendPage(const void *data)
{
	if(IsFree()==0)
		return -1;

	file.seekg(0,ios::end);

	file.write((char*)data,PF_PAGE_SIZE);
	
	
    return 0;
}


unsigned PF_FileHandle::GetNumberOfPages()
{

	if(IsFree()==0)
		return 0;

	int curr_length;

	file.seekg(0,ios::end);
	
	curr_length = file.tellg();

	int nop = curr_length/4096;

	return nop;
}


