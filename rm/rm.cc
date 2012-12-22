


#include "rm.h"

#include<string.h>
#include<stdlib.h>
#include<malloc.h>
#include<iostream>

void fill_slot_directory(struct SlotDirectory &S,const void *data);

int get_table_ID(const string tableName);


void fill_slot_directory(struct SlotDirectory &S,const void *data)
{
	memcpy(&S.fspace,(char*)data+4092,sizeof(int));
	memcpy(&S.nos,(char*)data+4088,sizeof(int));
	//cout<<"******IN fll slot dir*****"<<endl;
	S.slots = new pairs[S.nos];

	for(int j=0;j<S.nos;j++)
	{
		memcpy(&S.slots[j].offset,(char*)data+4080-8*j,sizeof(int));
		memcpy(&S.slots[j].length,(char*)data+4084-8*j,sizeof(int)); 
	}

	
}

void sort(int x[],int n)
{
	int i,j,tmp;

	for(i=1;i<n;i++)
	{
		tmp = x[i];

		for(j=i-1;j>=0 && x[j]>tmp;j--)
			x[j+1]=x[j];

		x[j+1]=tmp;
	}
}

int find_index(int x,struct SlotDirectory S)
{
	int i;

	for(i=0;i<S.nos;i++)
	{
		if(x==S.slots[i].offset)
			return i;
	}

	return -1;
}

int get_tuplesize(const void *data,vector<Attribute> &attrs)
{
	int i;
	int offset=0;
	int x;

	for(i=0;i<(int)attrs.size();i++)
	{
		//cout<<"Running attribute :"<<attrs.at(i).name<<endl;

		if(attrs.at(i).type == TypeVarChar)
		{
			//cout<<"TypeVarChar got...."<<endl;

			memcpy(&x,(char*)data+offset,sizeof(int));

			offset+=sizeof(int);

			offset+=x;	
		}
		else
		{
			offset+=sizeof(int);	
		}
	}	

	return offset;
}

RM* RM::_rm = 0;

RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

void RM::createCatalogs()
{

	int offset=0,offset1=0;

	int rc = pf->CreateFile("Tables");

	if(rc == -1)
		return;

	rc = pf->CreateFile("Columns");

	if(rc == -1)
		return;

	int tc1=1,tc2=2;
	
	RID rid;

	void *data = malloc(100);
	memcpy((char *)data+offset,&tc1,sizeof(int));
	offset=offset+sizeof(int);
	int b = 6;

	memcpy((char *)data+offset,&b,sizeof(int));
	offset=offset+sizeof(int);

	char x[]="Tables";

	memcpy((char *)data+offset,x,b);
	offset=offset+b;

	memcpy((char *)data+offset,&b,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,x,b);
	offset=offset+b;

	rc = insertTuple("Tables",data,rid,offset);


	void *data1=malloc(100);

	memcpy((char *)data1+offset1,&tc2,sizeof(int));
	offset1=offset1+sizeof(int);
	int c = 7;

	memcpy((char *)data1+offset1,&c,sizeof(int));
	offset1=offset1+sizeof(int);

	char x1[]="Columns";

	memcpy((char *)data1+offset1,x1,c);
	offset1=offset1+c;

	memcpy((char *)data1+offset1,&c,sizeof(int));
	offset1=offset1+sizeof(int);

	memcpy((char *)data1+offset1,x1,c);
	offset1=offset1+c;

	rc = insertTuple("Tables",data1,rid,offset1);

	vector<Attribute> attrs;
	vector<Attribute> attrs1;

	Attribute attrtable,attrcol;

	attrtable.name = "Table-Id";
	attrtable.type = TypeInt;
	attrtable.length = (AttrLength)4;
	attrs.push_back(attrtable);
	attrtable.name = "TableName";
	attrtable.type = TypeVarChar;
	attrtable.length = (AttrLength)20;
	attrs.push_back(attrtable);
	attrtable.name = "FileName";
	attrtable.type = TypeVarChar;
	attrtable.length = (AttrLength)20;
	attrs.push_back(attrtable);


	attrcol.name = "Table-Id";
	attrcol.type = TypeInt;
	attrcol.length = (AttrLength)4;
	attrs1.push_back(attrcol);
	attrcol.name = "ColumnName";
	attrcol.type = TypeVarChar;
	attrcol.length = (AttrLength)20;
	attrs1.push_back(attrcol);
	attrcol.name = "Column-Type";
	attrcol.type = TypeVarChar;
	attrcol.length = (AttrLength)4;
	attrs1.push_back(attrcol);

	attrcol.name = "Column-Length";
	attrcol.type = TypeInt;
	attrcol.length = (AttrLength)4;
	attrs1.push_back(attrcol);


	void *data2=malloc(100);
	void *data3=malloc(100);

	for(int i=0;i<(int)attrs.size();i++)

	{
		offset=0;
		memcpy((char *)data2+offset,&tc1,sizeof(int));
		offset=offset+sizeof(int);

		int a = (attrs.at(i).name).length();

		memcpy((char *)data2+offset,&a,sizeof(int));
		offset=offset+sizeof(int);

		memcpy((char *)data2+offset,attrs.at(i).name.c_str(),a); 
		offset=offset+a;

		int y=attrs.at(i).type;
		memcpy((char *)data2+offset,&y,sizeof(int));
		offset=offset+sizeof(int);

		int l=attrs.at(i).length;
		memcpy((char *)data2+offset,&l,sizeof(int));
		offset=offset+sizeof(int);
		rc = insertTuple("Columns",data2,rid,offset);

	}

	for(int j=0;j<(int)attrs1.size();j++)

	{
		offset=0;
		memcpy((char *)data3+offset,&tc2,sizeof(int));
		offset=offset+sizeof(int);

		int a = (attrs1.at(j).name).length();

		memcpy((char *)data3+offset,&a,sizeof(int));
		offset=offset+sizeof(int);

		memcpy((char *)data3+offset,attrs1.at(j).name.c_str(),a); 
		offset=offset+a;

		int y=attrs1.at(j).type;
		memcpy((char *)data3+offset,&y,sizeof(int));
		offset=offset+sizeof(int);

		int l=attrs1.at(j).length;
		memcpy((char *)data3+offset,&l,sizeof(unsigned));
		offset=offset+sizeof(unsigned);
		rc = insertTuple("Columns",data3,rid,offset);

	}

	free(data);
	free(data1);
	free(data2);
	free(data3);
}


RM_ScanIterator::RM_ScanIterator()
{
	pf = PF_Manager::Instance();
	iterator=0;
}

RC RM_ScanIterator::getNextTuple(RID &rid,void *data)
{
	if(iterator == (int)rids.size())
		return RM_EOF;

	rid = rids.at(iterator);

	memcpy((char*)data,(char*)data_projection.at(iterator),lengths.at(iterator));

	////cout<<"TEST :"<<data_projection.at(iterator).length()<<endl;


	iterator++;

	return 1;
}

void RM_ScanIterator::addAttribute(Attribute attr)
{
	attrs.push_back(attr);
}

void RM_ScanIterator::addRid(RID rid)
{
	rids.push_back(rid);
}

void RM_ScanIterator::addData(void *projection,int length)
{
	void * tempCopy = malloc( length );
	memcpy( ( char * )tempCopy, ( const char * )projection, length );
	data_projection.push_back(tempCopy);
	lengths.push_back(length);

	////cout<<"TEST:"<<projection_str.length()<<endl;
}

void RM_ScanIterator::resetState()
{
	iterator=0;

	for( std::vector< void * > ::iterator iter = data_projection.begin(); iter != data_projection.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}

	attrs.clear();
	rids.clear();
	data_projection.clear();
}

int project(const string attribute,vector<string> attributeNames)
{
	int i;

	for(i=0;i<(int)attributeNames.size();i++)
	{
		if(attribute == attributeNames.at(i))
			return 0;
	}

	return -1;
}

RC RM::scan(const string tableName,const string conditionAttribute,const CompOp compOp,const void *value,const vector<string> &attributeNames,RM_ScanIterator &rm_ScanIterator)
{
	rm_ScanIterator.resetState();


	PF_FileHandle fileHandle;

	int rc;

	rc = pf->OpenFile(tableName.c_str(),fileHandle);

	SlotDirectory S;

	int i,nop,j,k,offset,x,pro_offset,y,flag;

	RID rid;

	nop = fileHandle.GetNumberOfPages();

	void *data = malloc(PF_PAGE_SIZE);

	void *record = malloc(1000);

	void *projection = malloc(PF_PAGE_SIZE);

	char *projection_cstring = (char*)malloc(1000);

	vector<Attribute> attrs;

	rc = getAttributes(tableName,attrs);

	for(k=0;k<(int)attrs.size();k++)
	{
		if(project(attrs.at(k).name,attributeNames)==0)
			rm_ScanIterator.addAttribute(attrs.at(k));
	}

	for(i=0;i<nop;i++)
	{
		rc = fileHandle.ReadPage(i,data);

		fill_slot_directory(S,data);

		for(j=0;j<S.nos;j++)
		{

			if(S.slots[j].offset>=4096)
                               continue;
			offset=0;			

			memcpy((char*)record,(char*)data+S.slots[j].offset,S.slots[j].length);

			flag=0;

			/*Check if conditionAttribute adheres to condition */

			for(k=0;k<(int)attrs.size() && flag!=1;k++)
			{
				if(conditionAttribute.length()==0)
					flag = 1;

				else if(attrs.at(k).name.compare(conditionAttribute)==0)
				{
					if(attrs.at(k).type!=TypeVarChar)
					{					
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						memcpy(&y,(char*)value,sizeof(int));

						switch(compOp)
						{
							case EQ_OP:
								if(x == y) flag=1;break;
							case LT_OP:
								if(x<y) flag = 1;;break;
							case GT_OP:
								if(x>y) flag = 1;break;
							case LE_OP:
								if(x<=y) flag = 1;break;
							case GE_OP:
								if(x>=y) flag = 1;break;
							case NE_OP:
								if(x!=y) flag = 1;break;
							case NO_OP:
								flag = 1;
						}
					}
					else
					{

						//cout<<"RIGHT POSITION!"<<endl;

						char *s1 =(char*) malloc(1000);
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						memcpy((char*)s1,(char*)record+offset,x);
						offset+=x;

						s1[x] = '\0';

						y = strcmp(s1,(char*)value);
						
						//cout<<"CHAR * VALUE :"<<(char*)value<<endl;

						switch(compOp)
						{
							case EQ_OP:
								if(y==0) flag=1;break;
							case LT_OP:
								if(y<0) flag = 1;;break;
							case GT_OP:
								if(y>0) flag = 1;break;
							case LE_OP:
								if(y<=0) flag = 1;break;
							case GE_OP:
								if(y>=0) flag = 1;break;
							case NE_OP:
								if(y!=0) flag = 1;break;
							case NO_OP:
								flag = 1;
						}


						free(s1);
					}
				}	
				else
				{
					if(attrs.at(k).type == TypeVarChar)
					{
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						offset+=x;

						
					}
					else
					{
						offset+=sizeof(int);
					}
				}					
			}

			if(flag == 0)
				continue;

			offset=0;
			pro_offset=0;

			/*Get each attr that is specified in attributeNames and write it to projection */

			rid.pageNum = i;
			rid.slotNum = j;

			rm_ScanIterator.addRid(rid);


			for(k=0;k<(int)attrs.size();k++)
			{
				if(project(attrs.at(k).name,attributeNames) == 0)
				{		
					//cout<<"TEST:"<<attrs.at(k).name<<endl;

					if(attrs.at(k).type == TypeVarChar)
					{
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);

						memcpy((char*)projection+pro_offset,&x,sizeof(int));
						pro_offset+=sizeof(int);

						memcpy((char*)projection+pro_offset,(char*)record+offset,x);
						offset+=x;
						pro_offset+=x;
					}
					else
					{
						memcpy((char*)projection+pro_offset,(char*)record+offset,sizeof(int));

						//int test;

						offset+=sizeof(int);
						pro_offset+=sizeof(int);
					}
				}
				
				else
				{
					if(attrs.at(k).type == TypeVarChar)
					{
						memcpy(&x,(char*)record+offset,sizeof(int));
						offset+=sizeof(int);
						offset+=x;

						
					}
					else
					{
						offset+=sizeof(int);
					}
				}

			}

//			memcpy(projection_cstring,(char*)projection,pro_offset);
			


//			projection_cstring[pro_offset]='\0';

//			//cout<<"TEST:"<<pro_offset<<endl;

			rm_ScanIterator.addData(projection,pro_offset);

			////cout<<"TEST:"<<pro_offset<<endl;
	
			int test;

			memcpy(&test,(char*)projection,sizeof(int));

			////cout<<"TEST : "<<test<<endl;

			memcpy(&test,(char*)projection+4,sizeof(int));
			
			////cout<<"TEST : "<<test<<endl;

			memcpy(&test,(char*)projection+8,sizeof(int));	

			////cout<<"TEST : "<<test<<endl;
		}	
	}

	free(projection);
	free(record);
	free(data);
	free(projection_cstring);
	rc = pf->CloseFile(fileHandle);

	return 0;
}

RM::RM()
{
	pf = PF_Manager::Instance(); // Update in rm.h to include the object

	//PF_PF_FileHandle PF_FileHandletab, PF_FileHandlecol; // Update in rm.h to include the object
/*
	tablecounter=0;

	vector<Attribute> attrs;
	vector<Attribute> attrs1;

	Attribute attrtable,attrcol;

	attrtable.name = "Table-Id";
	attrtable.type = TypeInt;
	attrtable.length = (AttrLength)4;
	attrs.push_back(attrtable);
	attrtable.name = "TableName";
	attrtable.type = TypeVarChar;
	attrtable.length = (AttrLength)20;
	attrs.push_back(attrtable);
	attrtable.name = "FileName";
	attrtable.type = TypeVarChar;
	attrtable.length = (AttrLength)20;
	attrs.push_back(attrtable);

	//cout<<"Hello1!"<<endl;

	int rc=createTable("Tables", attrs);

	//cout<<"Hello2!"<<endl;
	
	attrcol.name = "Table-Id";
	attrcol.type = TypeInt;
	attrcol.length = (AttrLength)4;
	attrs1.push_back(attrcol);
	attrcol.name = "ColumnName";
	attrcol.type = TypeVarChar;
	attrcol.length = (AttrLength)20;
	attrs1.push_back(attrcol);
	attrcol.name = "Column-Type";
	attrcol.type = TypeVarChar;
	attrcol.length = (AttrLength)4;
	attrs1.push_back(attrcol);

	attrcol.name = "Column-Length";
	attrcol.type = TypeInt;
	attrcol.length = (AttrLength)4;
	attrs1.push_back(attrcol);

	
	rc = createTable("Columns",attrs1);

	//cout<<"Hello3!"<<endl;

*/
	tablecounter=2;
	createCatalogs();
}

RM::~RM()
{
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs)
{
	int offset=0,rc=0;

	rc = pf->CreateFile(tableName.c_str());

	if(rc== -1)
		return rc;


	RID rid;

	tablecounter=tablecounter+1;
	
		
	void *data = malloc(1000);
	memcpy((char *)data+offset,&tablecounter,sizeof(int));
	offset=offset+sizeof(int);
	int y = tableName.length();

	memcpy((char *)data+offset,&y,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tableName.c_str(),y);
	offset=offset+y;

	memcpy((char *)data+offset,&y,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tableName.c_str(),y);
	offset=offset+y;

	rc = insertTuple("Tables",data,rid,offset);
	//cout<<"*****Data entered in tablectr is "<<tablecounter<<endl;
	free(data);		
	
	//cout<<"Hello4!"<<endl;


	void *data1 = malloc(100);

	for(int i=0;i<(int)attrs.size();i++)
	{
		offset =0;
		memcpy((char *)data1+offset,&tablecounter,sizeof(int));
		offset=offset+sizeof(int);

		int a = (attrs.at(i).name).length();

		memcpy((char *)data1+offset,&a,sizeof(int));
		offset=offset+sizeof(int);

		memcpy((char *)data1+offset,attrs.at(i).name.c_str(),a); 
		offset=offset+a;

		int y=attrs.at(i).type;
		memcpy((char *)data1+offset,&y,sizeof(int));
		offset=offset+sizeof(int);

		int l=attrs.at(i).length;
		memcpy((char *)data1+offset,&l,sizeof(unsigned));
		offset=offset+sizeof(unsigned);

		//cout<<"Hello6!"<<endl;

		rc = insertTuple("Columns",data1,rid,offset);

		//cout<<"Hello7!"<<endl;
	}

	free(data1);

	//cout<<"Hello5!"<<endl;

	return rc;
}

RC RM::deleteTable(const string tableName)
{
	int rc=pf->DestroyFile(tableName.c_str());

	if(rc == -1)
		return rc;

	int tid = get_table_ID(tableName);

	PF_FileHandle fileHandle;

	rc = pf->OpenFile("Tables",fileHandle);

	int nop = fileHandle.GetNumberOfPages();

	struct SlotDirectory S;

	void *data = malloc(PF_PAGE_SIZE);

	int j,x,y,i;

	for(i=0;i<nop;i++)
	{
		rc = fileHandle.ReadPage(i,data);

		fill_slot_directory(S,data);

		void *datatmp = malloc(100);

		for(j=0;j<S.nos;j++)
		{
			memcpy((char*)datatmp,(char*)data+S.slots[j].offset,S.slots[j].length);

			memcpy(&x,(char*)datatmp,sizeof(int));

			if(x == tid)
			{
				y = 4096+S.slots[j].offset;

				memcpy((char*)data+4080-8*j,&y,sizeof(int));

				int z;

				memcpy(&z,(char*)data+4080-8*j,sizeof(int));

				//cout<<"Z:"<<z<<endl;
			}	
			
		}

		rc = fileHandle.WritePage(i,data);

		free(datatmp);
		delete []S.slots;
	}


	rc = pf->CloseFile(fileHandle);		

	rc = pf->OpenFile("Columns",fileHandle);

	nop = fileHandle.GetNumberOfPages();		

	for(i=0;i<nop;i++)
	{
		rc = fileHandle.ReadPage(i,data);

		fill_slot_directory(S,data);

		void *datatmp = malloc(100);

		for(j=0;j<S.nos;j++)
		{
			memcpy((char*)datatmp,(char*)data+S.slots[j].offset,S.slots[j].length);

			memcpy(&x,(char*)datatmp,sizeof(int));

			if(x == tid)
			{
				y = 4096+S.slots[j].offset;

				memcpy((char*)data+4080-8*j,&y,sizeof(int));

				int z;

				memcpy(&z,(char*)data+4080-8*j,sizeof(int));

				//cout<<"Z:"<<z<<endl;


			}	
			
		}

		rc = fileHandle.WritePage(i,data);

		free(datatmp);
		delete []S.slots;
	}


	
	rc = pf->CloseFile(fileHandle);

	free(data);

	return rc;
}


RC RM::getAttributes(const string tableName, vector<Attribute> &attrs)
{
		/* Remember that the catalogs are open */

		/* select * from columns,tables where tables.table-name = 'tableName' and columns.table_id = tables.table-id */

		/* fill &attrs with columns.column-name, columns.column-type and columns.column-length */

		PF_FileHandle fileHandle;

		int r = pf->OpenFile(tableName.c_str(),fileHandle);

		if(r == -1)
		{
			pf->CloseFile(fileHandle);
			return -1;
		}

		pf->CloseFile(fileHandle);

		attrs.clear();

		int rc,tid1,tid2;
		//cout<<"AM atleast here"<<endl;
		tid1 = get_table_ID(tableName);
		//cout<<"Table ID is : "<<tid1<<endl;

		int i,nop;

		int col_name_length,col_type,col_length;

		char *col_name = (char*)malloc(100);

		rc = pf->OpenFile("Columns",fileHandle);

		void *data = malloc(PF_PAGE_SIZE);

		int offset,j;

		Attribute attr;

		nop = fileHandle.GetNumberOfPages();

		for(i=0;i<nop;i++)
		{
			rc = fileHandle.ReadPage(i,data);

			struct SlotDirectory S;

			fill_slot_directory(S,data);


			for(j=0;j<S.nos;j++)
			{						
				offset = S.slots[j].offset;

				memcpy(&tid2,(char*)data+offset,sizeof(int));

				offset+=sizeof(int);
				
				if(tid2==tid1)
				{
					memcpy(&col_name_length,(char*)data+offset,sizeof(int));

					offset+=sizeof(int);

					memcpy(col_name,(char*)data+offset,col_name_length);

					col_name[col_name_length]='\0';

					attr.name = col_name;

					offset+=col_name_length;

					memcpy(&col_type,(char*)data+offset,sizeof(int));

					offset+=sizeof(int);

					attr.type = (AttrType)col_type;

					memcpy(&col_length,(char*)data+offset,sizeof(int));

					offset+=sizeof(int);

					attr.length = col_length;

					attrs.push_back(attr);

						
				}
				
			}

			delete []S.slots;
		}

		rc = pf->CloseFile(fileHandle);

		free(data);
		free(col_name);

		return rc;	
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid,int tuplesize)
{

	////cout<<"Starting insert...."<<endl;

	PF_FileHandle fileHandle;

	int rc;

	rc = pf->OpenFile(tableName.c_str(),fileHandle);

	if(rc == -1)
		return rc;

	int nop = fileHandle.GetNumberOfPages();

	////cout<<"TNAME + NOP :"<<tableName<<","<<nop<<endl;

	int flag=0;

	vector<Attribute> attrs;
	
	////cout<<"TupleSize....."<<endl;	

	if(tuplesize == 0)
	{
		rc = getAttributes(tableName.c_str(),attrs);

		tuplesize = get_tuplesize(data,attrs);

		//cout<<"Got tuplesize...."<<endl;
	}

	void *datatmp = malloc(PF_PAGE_SIZE);

	struct SlotDirectory S;

	//cout<<"Starting page iteration...Nop :"<<nop<<endl;

	for(int i=0;i<nop && flag==0;i++)
	{
		/*find first available page with enough free space */
		rc = fileHandle.ReadPage(i,datatmp);

		//memcpy(&free,((char*)datatmp+4092),sizeof(int));

		fill_slot_directory(S,datatmp);		//Verify slot dir***************//


		
		
		int j,slotflag=0,readptr = -1;


		for(j=0;j<S.nos && slotflag==0;j++) /*either exhaust list of slots or hit a slot of a deleted record large enough to accommodate tuple */
		{
			if(S.slots[j].offset>=4096) /*if there is a slot pointing to a deleted record */
			{
				

				if(S.slots[j].length >= tuplesize)	/*if slot is big enough to hold the tuple */
				{
					slotflag=1;
					
					//cout<<"Deleted record slot :"<<j<<" in page :"<<i<<endl;

					/*
					if(j==0)	
					{
						readptr = 0; 
						S.slots[j].offset = 0;
							
					}
					else	
					{
						int correct_offset=0;

						int k=j,l=j-1;

						int offsetflag=0;

						while(k!=0 && offsetflag==0)
						{
							if(S.slots[l].offset == -1) 
							{
								correct_offset+=S.slots[l].length;
								k--;l--;
							}
							else
							{
								correct_offset+=S.slots[l].offset+S.slots[l].length; 
							}
						}

						readptr = correct_offset;
					}
					*/
					S.slots[j].offset-=4096;

					//cout<<"Made slot <<"<<j<<" valid"<<endl;
					//cout<<"New offset of slot is "<<S.slots[j].offset<<endl;

					S.slots[j].length = tuplesize; /* update slot length to new tuple size */
				}
			}
		}

		if(slotflag == 1) /* tuple has found a slot that was previously holding a deleted record */
		{
			j--;
			memcpy((char*)datatmp+S.slots[j].offset,(char*)data,tuplesize);
			memcpy((char*)datatmp+4080-8*j,&S.slots[j].offset,sizeof(int));
			memcpy((char*)datatmp+4084-8*j,&S.slots[j].length,sizeof(int));

			//cout<<"Value of slot :"<<j<<endl;

			rid.pageNum = i;
			rid.slotNum = j;

			//cout<<"OFFSET during insertion :"<< S.slots[j].offset<<endl;

			flag = 1;
		}

		else
		{
			int size_of_slot_directory = 8 + S.nos*8;
			if(S.fspace+tuplesize<=4096-size_of_slot_directory-8)
			{
				readptr = S.fspace;
				memcpy((char*)datatmp+readptr,(char*)data,tuplesize);
				memcpy((char*)datatmp+4080-8*j,&readptr,sizeof(int));

				//cout<<"OFFSET during insertion:"<<readptr<<endl;

				memcpy((char*)datatmp+4084-8*j,&tuplesize,sizeof(int));
	
				S.fspace+=tuplesize;

				memcpy((char*)datatmp+4092,&S.fspace,sizeof(int));
				
				rid.pageNum = i;
				rid.slotNum = S.nos;

				S.nos++;

				memcpy((char*)datatmp+4088,&S.nos,sizeof(int));

				flag = 1;
			}
		}

		delete []S.slots;	

		rc = fileHandle.WritePage(i,datatmp);			
	}
		
		
	if(flag==0)
	{
		int n=1;
		memcpy((char*)datatmp,(char*)data,tuplesize);
		memcpy((char*)datatmp+4092,&tuplesize,sizeof(int));
		memcpy((char*)datatmp+4088,&n,sizeof(int));
		
		n=0;

		memcpy((char*)datatmp+4080,&n,sizeof(int));
		memcpy((char*)datatmp+4084,&tuplesize,sizeof(int));		

		rc = fileHandle.AppendPage(datatmp);
		rid.pageNum = nop;
		rid.slotNum = 0;

		//cout<<"Appending Page...."<<endl;
	}

	rc = pf->CloseFile(fileHandle);

	free(datatmp);

	//cout<<"SlotNum during insertion:"<<rid.slotNum<<endl;

	return rc;
}

RC RM::deleteTuples(const string tableName)
{
	PF_FileHandle fileHandle;

	int rc;

	rc = pf->OpenFile(tableName.c_str(),fileHandle);

	if(rc == -1)
		return rc;

	void *data = malloc(PF_PAGE_SIZE);

	int fspace=0,nos=0;

	memcpy((char*)data+4092,&fspace,sizeof(int));
	memcpy((char*)data+4088,&nos,sizeof(int));

	int nop,i;	

	nop = fileHandle.GetNumberOfPages();

	for(i=0;i<nop;i++)
	{
		rc=fileHandle.WritePage(i,data);
	}
	rc = pf->CloseFile(fileHandle);

	free(data);

	return rc;
} 

RC RM::deleteTuple(const string tableName, const RID &rid)
{
	PF_FileHandle fileHandle;

	int rc;

	rc = pf->OpenFile(tableName.c_str(),fileHandle);

	if(rc == -1)
		return rc;

	void *data = malloc(PF_PAGE_SIZE);

	rc = fileHandle.ReadPage(rid.pageNum,data);

	if(rc == -1)
		return -1;

	struct SlotDirectory S;

	fill_slot_directory(S,data);

	if(S.nos<=(int)rid.slotNum)
		return -1;

	if(S.slots[rid.slotNum].offset>=4096)
		return -1;

	int delete1 = S.slots[rid.slotNum].offset+4096;

	memcpy((char*)data+4080-8*rid.slotNum,&delete1,sizeof(int));

	rc = fileHandle.WritePage(rid.pageNum,data);

	free(data);

	rc = pf->CloseFile(fileHandle);
	
	delete []S.slots;

	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data)
{
	int rc;

	PF_FileHandle fileHandle;

	rc = pf->OpenFile(tableName.c_str(),fileHandle);

	if(rc == -1)
		return rc;

	////cout<<"Read Tuple Hello1!"<<endl;

	void *datatmp = malloc(PF_PAGE_SIZE);

	rc = fileHandle.ReadPage(rid.pageNum,datatmp);

	if(rc == -1)
		return -1;

	SlotDirectory S;

	fill_slot_directory(S,datatmp);

	if(rid.slotNum>=(unsigned)S.nos)
		return -1;

	int length;

	memcpy(&length,(char*)datatmp+4084-8*rid.slotNum,sizeof(int));

	int offset;

	memcpy(&offset,(char*)datatmp+4080-8*rid.slotNum,sizeof(int));

	memcpy((char*)data,(char*)datatmp+offset,length);

	free(datatmp);
	rc = pf->CloseFile(fileHandle);


	int name_length;

	memcpy(&name_length,(char*)data,sizeof(int));

	//cout<<"NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN :"<<name_length<<endl;

	if(offset>=4096)
		return -1;

	return 0;
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data)
{
	vector<Attribute> attrs;

	int rc = getAttributes(tableName,attrs);

	PF_FileHandle fileHandle;

	rc = pf->OpenFile(tableName.c_str(),fileHandle);

	void *datatmp = malloc(PF_PAGE_SIZE);

	rc = fileHandle.ReadPage(rid.pageNum,datatmp);

	int offset;

	memcpy(&offset,(char*)datatmp+4080-rid.slotNum*8,sizeof(int));

	int i,flag=0,x=-1;

	//cout<<"Slot Num :"<<rid.slotNum<<endl;

	//cout<<"OFFSET:"<<offset<<endl;

	for(i=0;i<(int)attrs.size() && flag == 0;i++)
	{
		if(attrs.at(i).type == TypeVarChar)
		{
			memcpy(&x,(char*)datatmp+offset,sizeof(int));

			offset+=sizeof(int);

			memcpy((char*)data,(char*)datatmp+offset,x);

			offset+=x;	
		}
		else
		{
			memcpy((char*)data,(char*)datatmp+offset,sizeof(int));
			offset+=sizeof(int);
			x = sizeof(int);	
		}

		if(attrs.at(i).name == attributeName)
		{
			flag=1;
		}
	
	}	

	rc = pf->CloseFile(fileHandle);

	free(datatmp);

	return 0;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid)
{
	PF_FileHandle fileHandle;

	int rc = pf->OpenFile(tableName.c_str(),fileHandle);

	void *datatmp = malloc(PF_PAGE_SIZE);

	struct SlotDirectory S;

	rc = fileHandle.ReadPage(rid.pageNum,datatmp);

	fill_slot_directory(S,datatmp);	

	vector<Attribute> attrs;

	rc = getAttributes(tableName,attrs);

	int tuplesize = get_tuplesize(data,attrs);

	//cout<<"Updated tuple size : " <<tuplesize<<endl;

	int offset,length;

	memcpy(&offset,(char*)datatmp+4080-8*rid.slotNum,sizeof(int));
	memcpy(&length,(char*)datatmp+4084-8*rid.slotNum,sizeof(int));

	int i;

	int new_end = offset+tuplesize;
	int old_end = offset+length;

	int flag=0;

	for(i=0;i<S.nos && flag == 0;i++)
	{
		if(S.slots[i].offset>=4096)
			S.slots[i].offset-=4096;
		
		if(i==(int)rid.slotNum)
			continue;
		else
		{
			if(S.slots[i].offset>=old_end && S.slots[i].offset<new_end)
				flag=1;
		}
	}

	if(flag==0)
	{
		memcpy((char*)datatmp+4084-8*rid.slotNum,&tuplesize,sizeof(int));
		memcpy((char*)datatmp+offset,(char*)data,tuplesize);
	}

	else
	{
		/*Check if free space in page can handle the updated tuple */

		int size_of_slot_directory = 8 + S.nos*8;

		if(S.fspace+tuplesize<=4096-size_of_slot_directory)
		{
			memcpy((char*)datatmp+S.fspace,(char*)data,tuplesize);
			memcpy((char*)datatmp+4080-8*rid.slotNum,&S.fspace,sizeof(int));
			memcpy((char*)datatmp+4084-8*rid.slotNum,&tuplesize,sizeof(int));
			
			S.fspace+=tuplesize;

			memcpy((char*)datatmp+4092,&S.fspace,sizeof(int));
		}	

		/*else write to next available page and leave a tombstone */
	}
		

	rc = fileHandle.WritePage(rid.pageNum,datatmp);

	free(datatmp);
	rc = pf->CloseFile(fileHandle);

	delete []S.slots;

	return 0;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber)
{
	PF_FileHandle fileHandle;

	int rc = pf->OpenFile(tableName.c_str(),fileHandle);


	void *data = malloc(PF_PAGE_SIZE);

	rc = fileHandle.ReadPage(pageNumber,data);

	if(rc == -1)
		return -1;

	struct SlotDirectory S1;

	fill_slot_directory(S1,data);

	int *x = new int[S1.nos];		
	
	int i;

	for(i=0;i<S1.nos;i++)
	{
		x[i]=S1.slots[i].offset;

		if(x[i]>=4096)
			x[i]-=4096;
	}

	sort(x,S1.nos);

	int index;

	int offset=0;

	void *new_data = malloc(PF_PAGE_SIZE);

	for(i=0;i<S1.nos;i++)
	{
		index = find_index(x[i],S1);

		if(index == -1)
			index = find_index(x[i]+4096,S1);

		memcpy((char*)new_data+4088,&S1.nos,sizeof(int));

		if(S1.slots[index].offset<4096)
		{
			memcpy((char*)new_data+offset,(char*)data+S1.slots[index].offset,S1.slots[index].length);
			memcpy((char*)new_data+4080-8*index,&offset,sizeof(int));
			memcpy((char*)new_data+4084-8*index,&S1.slots[index].length,sizeof(int));
		
			offset+=S1.slots[index].length;			
		}
		else
		{
			int tmp = 4096+offset;
			memcpy((char*)new_data+4080-8*index,&tmp,sizeof(int));
			memcpy((char*)new_data+4084-8*index,&S1.slots[index].length,sizeof(int));
			offset+=S1.slots[index].length;
		}
	}

	memcpy((char*)new_data+4092,&offset,sizeof(int));

	rc = fileHandle.WritePage(pageNumber,new_data);

	delete []x;
	free(data);
	free(new_data);
	rc = pf->CloseFile(fileHandle);
	delete []S1.slots;

	return rc;
}

int RM::get_table_ID(const string tableName)
{
	PF_FileHandle fileHandle1;

	int rc = pf->OpenFile("Tables",fileHandle1);

	int i,nop,j,flag=0;

	nop = fileHandle1.GetNumberOfPages();

	void *data = malloc(PF_PAGE_SIZE);

	char *tName = (char*)malloc(1000);

	int tid,stname;
	//cout<<"******HI here too *** "<<endl;
	for(i=0;i<nop && flag==0;i++)
	{
		rc = fileHandle1.ReadPage(i,data);

		struct SlotDirectory S;

		fill_slot_directory(S,data);
		//cout<<"****outside fill****"<<endl;
		int offset1 = 0;
		for(j=0;j<S.nos && flag==0;j++)
		{			
			
			
			offset1 = S.slots[j].offset;

			//following two lines have been added from the previously submitted rm.cc file

			if(offset1>4096)
			continue;

			memcpy(&tid,(char*)data+offset1,sizeof(int));
			
			offset1+=sizeof(int);
		
			memcpy(&stname,(char*)data+offset1,sizeof(int));
			
			offset1+=sizeof(int);

			//cout<<"**start of loop**"<<endl; cout<<"***val-offset is :"<<offset1<<"  ** val of stname is: "<<stname<<" **tid is : "<<tid<<endl;
			memcpy(tName,(char*)data+offset1,stname);
			
			tName[stname]='\0';

			if(strcmp(tableName.c_str(),tName)==0)
				flag=1;				
			//cout<<"inside for *****"<<endl;
			//cout<<"**val of flag : "<<flag<<endl;
		}
		//cout<<"***outside for loop***"<<endl;
		delete []S.slots;
	}

	
	//cout<<"***before***"<<endl;
	free(data);
	free(tName);
	//cout<<"***aftre freein***"<<endl;
	rc = pf->CloseFile(fileHandle1);

	if(flag==0)
		return -1;
	
	return tid;
}

