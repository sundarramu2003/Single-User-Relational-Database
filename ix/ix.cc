
#include <iostream>

using namespace std;

#include "ix.h"

PF_Manager *pf=PF_Manager::Instance();


IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance()
{
    if(!_ix_manager)
        _ix_manager = new IX_Manager();
    
    return _ix_manager;    
}

IX_Manager::IX_Manager()
{

}

IX_Manager::~IX_Manager()
{
}


RC IX_Manager::CreateIndex(const string tableName,const string attributeName)
{
	RM *rm = RM::Instance();

	string name = tableName+"."+attributeName;

	char fileName[30];

	strcpy(fileName,name.c_str());

	int x = pf->CreateFile(fileName);

	if(x!=0)
		return x;
	
	PF_FileHandle fileHandle;

	x = pf->OpenFile(fileName,fileHandle);

	if(x!=0)
		return x;

	void *data = malloc(PF_PAGE_SIZE);

	int root = 0;

	int m = dd;
	
	memcpy((char*)data,&root,sizeof(int));

	/*Code to get attribute type starts here */

	vector<Attribute> attrs;

	x = rm->getAttributes(tableName,attrs);

	if(x!= 0 )
		return x;

	AttrType type;

	for(unsigned i=0;i<attrs.size();i++)
	{
		if(attrs[i].name == attributeName)
			type = attrs[i].type;		
	}

	memcpy((char*)data+4,&type,sizeof(type));

	/*Code to get attribute type ends here */

	memcpy((char*)data+8,&m,sizeof(int));

	x = fileHandle.AppendPage(data);

	x = pf->CloseFile(fileHandle);

	free(data);

	return x;
}

RC IX_Manager::DestroyIndex(const string tableName,const string attributeName)
{
	string name = tableName+"."+attributeName;

	char fileName[30];

	strcpy(fileName,name.c_str());

	return pf->DestroyFile(fileName);
}

RC IX_Manager::OpenIndex(const string tableName,const string attributeName,IX_IndexHandle &indexHandle)
{
	string name = tableName+"."+attributeName;

	char fileName[30];

	strcpy(fileName,name.c_str());

	if(indexHandle.IsFree()==0)
		return indexHandle.SetHandle(fileName);
	else
		return -1;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle)
{
    return indexHandle.DropHandle();
}

IX_IndexHandle::IX_IndexHandle()
{
}

IX_IndexHandle::~IX_IndexHandle()
{
		int x = pf->CloseFile(fileHandle);
}

RC IX_IndexHandle::SetHandle(const char *fileName)
{
	return fileHandle.SetHandle(fileName);
}

RC IX_IndexHandle::DropHandle()
{
	return fileHandle.DropHandle();
}

RC IX_IndexHandle::IsFree()
{
	return fileHandle.IsFree();
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid)
{
	if(IsFree()==0)
		return 1;	

	int nodepointer,flag=0;

	void *data = malloc(PF_PAGE_SIZE);	

	int x = fileHandle.ReadPage(0,data);

	memcpy(&nodepointer,(int*)data,sizeof(int));	

	int page = -1;

	int fp = -1,bp = -1;

	/*Code for attribute type differentiation starts here */

	AttrType type;

	memcpy(&type,(char*)data+4,sizeof(type));

	int kInt,newchildentryInt = -1;
	float kFloat,newchildentryFloat = -1;

	if(type == TypeInt)
	{
		memcpy(&kInt,(char*)key,sizeof(kInt));
		flag = insert(nodepointer,kInt,rid,newchildentryInt,page,fp,bp);
	}
	else if(type == TypeReal)
	{
		memcpy(&kFloat,(char*)key,sizeof(kFloat));
		//cout<<kFloat<<endl;
		flag = insert(nodepointer,kFloat,rid,newchildentryFloat,page,fp,bp);
	}

	/*Code for attribute type differentiation ends here */

	free(data);

	if(flag == 1)
		return 1;
	
	return 0;

}

void IX_IndexHandle::update_book(int root)
{
	void *data2 = malloc(PF_PAGE_SIZE);

	int x = fileHandle.ReadPage(0,data2);

	memcpy((char*)data2,&root,sizeof(root));

	x = fileHandle.WritePage(0,data2);

	free(data2);
}
template<class T>
RC IX_IndexHandle::insert(int nodepointer,T k,const RID &rid,T &newchildentry,int &page,int fp,int bp)
{
	//cout<<"nodepointer:"<<nodepointer<<endl;

	//cout<<"key:"<<(int*)key<<endl;

	//cout<<"rid:{"<<rid.pageNum<<","<<rid.slotNum<<"}"<<endl;

	//cout<<"newchildentry:"<<newchildentry<<endl;

	//cout<<"page:"<<page<<endl;

	//cout<<"nodepointer:"<<nodepointer<<endl;

	int flag;
	int exists = 0;

	if(nodepointer < -1)
	{
		leaf<T> L;

		L.keys[0] = k;
		L.rids[0] = rid;

		L.n++;

		L.fp = fp;
		L.bp = bp;

		void *data = malloc(PF_PAGE_SIZE);

		int x = 1;

		memcpy((char*)data,&x,sizeof(x));

		memcpy((char*)data+4,&L,sizeof(L));

		nodepointer = -(nodepointer);

		x = fileHandle.WritePage(nodepointer,data);

		page = nodepointer;

		if(fp!=-1)
		{

			//cout<<"HIT!"<<endl;

			x = fileHandle.ReadPage(fp,data);		

			//cout<<"FP:"<<fp<<endl;
			
			//cout<<"BP:"<<bp<<endl;
	
			memcpy(&L,(char*)data+4,sizeof(L));

			L.bp = nodepointer;

			//cout<<"PAGE:"<<L.bp<<endl;

			memcpy((char*)data+4,&L,sizeof(L));

			x = fileHandle.WritePage(fp,data);

			//cout<<"X:"<<x<<endl;
		}

		if(bp!=-1)
		{
			x = fileHandle.ReadPage(bp,data);

			memcpy(&L,(char*)data+4,sizeof(L));

			L.fp = nodepointer;
			
			memcpy((char*)data+4,&L,sizeof(L));

			x = fileHandle.WritePage(bp,data);
		}


		free(data);

		return exists;										
		
	}

	if(nodepointer == -1)
	{
		leaf<T> L;

		L.keys[0] = k;

		//cout<<L.keys[0]<<endl;

		L.rids[0].pageNum = rid.pageNum;
		L.rids[0].slotNum = rid.slotNum;

		L.n++;

		L.fp = fp;
		L.bp = bp;

		void *data = malloc(PF_PAGE_SIZE);

		flag = 1;

		memcpy((char*)data,&flag,sizeof(int));

		memcpy((char*)data+4,&L,sizeof(L));

		//display_leaf(L);

		int nop = fileHandle.GetNumberOfPages();

		int x = fileHandle.AppendPage(data);

		page = nop;

		/* Code to set fp page's bp */

		if(fp!=-1)
		{

			//cout<<"HIT!"<<endl;

			x = fileHandle.ReadPage(fp,data);		

			//cout<<"FP:"<<fp<<endl;
			
			//cout<<"BP:"<<bp<<endl;
	
			memcpy(&L,(char*)data+4,sizeof(L));

			L.bp = page;

			//cout<<"PAGE:"<<L.bp<<endl;

			memcpy((char*)data+4,&L,sizeof(L));

			x = fileHandle.WritePage(fp,data);

			//cout<<"X:"<<x<<endl;
		}

		if(bp!=-1)
		{
			x = fileHandle.ReadPage(bp,data);

			memcpy(&L,(char*)data+4,sizeof(L));

			L.fp = page;
			
			memcpy((char*)data+4,&L,sizeof(L));

			x = fileHandle.WritePage(bp,data);
		}


		//cout<<"PAGE :"<<page<<endl;

		free(data);

		return exists;		
		
	}

	if(nodepointer == 0)
	{
		internals<T> N;

		N.keys[N.n] = k; //memcpy(&N.keys[N.n],(int*)key,sizeof(int));
		
		N.n++;

		N.ptrs[1] = 2;

		leaf<T> L;
	
		L.keys[L.n] = k;//memcpy(&L.keys[L.n],(int*)key,sizeof(int));
		
		L.rids[L.n] = rid;

		L.n++;		

		void *data = malloc(PF_PAGE_SIZE);

		int flag = 0;

		memcpy((int*)data,&flag,sizeof(flag));		

		memcpy((char*)data+4,&N,sizeof(N));

		fileHandle.AppendPage(data);

		flag = 1;

		memcpy((int*)data,&flag,sizeof(int));

		memcpy((char*)data+4,&L,sizeof(L));

		fileHandle.AppendPage(data);

		nodepointer = 1;

		update_book(nodepointer);

		free(data);

		return exists;
	}

	void *data = malloc(PF_PAGE_SIZE);

	int x = fileHandle.ReadPage(nodepointer,data);

	//node tmp;

	memcpy(&x,(int*)data,sizeof(int));

	int j;

	int root = 0;
/*
	int k;

	memcpy(&k,(int*)key,sizeof(int));		
*/
	/*if(x == 0)
		cout<<"Trying to insert <<"<<k<<" into Node"<<endl;
	else
		cout<<"Trying to insert <<"<<k<<" into Leaf"<<endl;
	*/
	//cout<<"STATE OK before processing node"<<endl;

	if(x == 0)	//internal node
	{
		//cout<<"ENTERED INTERNAL NODE!"<<endl;
		
		internals<T> N;

		memcpy(&N,(char*)data+4,sizeof(N));

		//display_node(N);

		
		int i;

		for(i=0;i<N.n && N.keys[i]<=k;i++);

		/* !!!!!!!!!!!!!!!!!!!!!!!!!!! Don't forget to handle the case when nodepointer = -1 !!!!!!!!!!!!!!!!!!!!!!!!!!!! */

		int oldnodepointer = nodepointer;

		nodepointer = N.ptrs[i];

		if(i < dd)
		{
			if(N.ptrs[i+1]!=-1)
			{
				fp = N.ptrs[i+1];
			}
			else if(fp!=-1)
			{
				void *data1 = malloc(PF_PAGE_SIZE);
				
				x = fileHandle.ReadPage(fp,data1);

				internals<T> tmp;

				memcpy(&tmp,(char*)data1+4,sizeof(tmp));

				fp = tmp.ptrs[0];

				free(data1);
			}
		}
		if(i > 0)
		{
			if(N.ptrs[i-1]!= -1)
			{
				bp = N.ptrs[i-1];
			}
			
			else if(bp!=-1)
			{
				void *data1 = malloc(PF_PAGE_SIZE);

				x = fileHandle.ReadPage(bp,data1);

				internals<T> tmp;

				memcpy(&tmp,(char*)data1+4,sizeof(tmp));

				bp = tmp.ptrs[tmp.n];

				free(data1);
			}
		}

		
		//cout<<"Node contents before trying to insert key :"<<k<<endl;

		//display_node(N);

		exists = insert(nodepointer,k,rid,newchildentry,page,fp,bp);

		if(exists == 1)
		{
			free(data);
			return exists;
		}

		//cout<<"HIT NODE!"<<endl;

		nodepointer = oldnodepointer;

		//if(newchildentry == 27)
			//cout<<"27 has come up!"<<endl;

		if(newchildentry == -1)
		{
			if(page!=-1)
			{
				N.ptrs[i] = page;

				//cout<<"Setting N.ptrs[i:"<<i<<"] to page :"<< page<<endl;
	
				memcpy((char*)data+4,&N,sizeof(N));

				x = fileHandle.WritePage(nodepointer,data);

				page = -1;
			}

			free(data);

			return 0;
		}

		if(N.n < dd)
		{
			/*			
			N.keys[N.n] = newchildentry;
			N.n++;
			newchildentry = -1;
			return;
			*/

			//cout<<"I:"<<i<<endl;

			for(j = N.n;j>i;j--)
			{
				N.keys[j] = N.keys[j-1];
				N.ptrs[j+1] = N.ptrs[j];
			}

			//cout<<"J:"<<j<<endl;

			N.ptrs[j+1] = page;

			page = -1;
		
			//cout<<"PAGE:"<<page<<endl;
			
			N.keys[j] = newchildentry;
			newchildentry = -1;			
			N.n++;

			memcpy((char*)data+4,&N,sizeof(N));

			x = fileHandle.WritePage(nodepointer,data);

			free(data);

			return 0;
		}

		//cout<<"111111"<<endl;

		internals<T> N2;

		T *keys;
		int *ptrs;

		keys = (T *)malloc((dd+1)*sizeof(T));
		ptrs = (int *)malloc((dd+2)*sizeof(int));

		for(int i=0;i<dd+1;i++)
		{
			ptrs[i] = -1;
			keys[i] = -1;
		}

		ptrs[i] = -1;

		//cout<<"222222"<<endl;

		for(int i=0;i<dd && N.keys[i]<=k;i++)
		{
			ptrs[i] = N.ptrs[i];
			keys[i] = N.keys[i];
		}

		ptrs[i] = N.ptrs[i];
		keys[i] = newchildentry;

		i++;

		ptrs[i] = page;

		//keys[i] = N.keys[i-1];

		//i++;

		//cout<<"333333"<<endl;

		for(;i<dd+1;i++)
		{
			keys[i] = N.keys[i-1];
			ptrs[i+1] = N.ptrs[i];
		}				

		//ptrs[i] = N.ptrs[i-1];

		internals<T> N1;

		//cout<<"444444"<<endl;

		for(i=0;i<d;i++)
		{
			N1.ptrs[i] = ptrs[i];
			N1.keys[i] = keys[i];
			N1.n++;
		}

		N1.ptrs[i] = ptrs[i];

		i++;

		//cout<<"55555"<<endl;

		for(i = d+1,j=0;i<dd+1;i++,j++)
		{
			N2.ptrs[j] = ptrs[i];
			N2.keys[j] = keys[i];
			N2.n++;
		}

		N2.ptrs[j] = ptrs[i];

		//cout<<"66666"<<endl;

		memcpy((char*)data+4,&N1,sizeof(N1));
		
		x = fileHandle.WritePage(nodepointer,data);

		memcpy((char*)data+4,&N2,sizeof(N2));		

		int nop = fileHandle.GetNumberOfPages();

		x = fileHandle.AppendPage(data);

		page = nop;

		newchildentry = keys[d];

		x = fileHandle.ReadPage(0,data);

		int root;

		memcpy(&root,(int*)data,sizeof(int));


		//cout<<"77777"<<endl;

		if(root == nodepointer)
		{
			//cout<<"ROOT=NODEPTR"<<endl;
		
			internals<T> N3;

			N3.keys[0] = keys[d];

			//cout<<"ROOT=NODEPTR2"<<endl;


			N3.ptrs[0] = nodepointer;
			N3.ptrs[1] = page;

			N3.n++;

			nop = fileHandle.GetNumberOfPages();

			flag = 0;

			memcpy((int*)data,&flag,sizeof(int));

			memcpy((char*)data+4,&N3,sizeof(N3));

			x = fileHandle.AppendPage(data);

			page = nop;

			//cout<<"ROOT3333"<<endl;

			update_book(page);

			//cout << "ROOT####"<<"\n";
		}		
						
		//cout<<"88888"<<endl;

		free(data);

		free(keys);
		free(ptrs);

		//cout<<"99999"<<endl;

		return 0;
	}

	else		//leaf node
	{
		//cout<<"ENTERED LEAF"<<endl;

		leaf<T> L;
		int i;
		memcpy(&L,(char*)data+4,sizeof(L));

		//cout<<"Leaf before inserting:"<<endl;

		//display_leaf(L);


		for(i=0;i<L.n;i++)
		if(L.keys[i]==k && L.rids[i].pageNum==rid.pageNum && L.rids[i].slotNum == rid.slotNum)
		{
			free(data);
			return 1;
		}

		if(L.n<dd)
		{
			for(i=0;i<L.n && L.keys[i]<=k;i++);

			if(i == L.n)
			{
				L.keys[L.n] = k;
				L.rids[L.n] = rid;
			}

			else
			{
				for(j=L.n;j>i;j--)
				{
					L.keys[j] = L.keys[j-1];
					L.rids[j] = L.rids[j-1];
				}

				L.keys[i] = k;
				L.rids[i] = rid;
			}

			L.n++;

			memcpy((char*)data+4,&L,sizeof(L));

			x = fileHandle.WritePage(nodepointer,data);

			newchildentry = -1;

			free(data);

			//display_leaf(L);

			return 0;
		}
		else
		{
			leaf<T> L2;

			T *keys = (T *)malloc((dd+1)*sizeof(T));
			RID *rids = (RID *)malloc((dd+1)*sizeof(RID));

			for(i=0;L.keys[i]<=k && i<L.n;i++)
			{
				keys[i] = L.keys[i];
				rids[i] = L.rids[i];
			}

			keys[i] = k;
			rids[i] = rid;

			i++;

			for(;i<dd+1;i++)
			{
				keys[i] = L.keys[i-1];
				rids[i] = L.rids[i-1];
			}
		
			leaf<T> L1;

			L1.fp = L.fp;
			L1.bp = L.bp;

			for(i=0;i<d;i++)
			{
				L1.keys[i] = keys[i];
				L1.rids[i] = rids[i];
				L1.n++;
			}

			for(j=0;i<dd+1;i++,j++)
			{
				L2.keys[j] = keys[i];
				L2.rids[j] = rids[i];
				L2.n++;
			}			

			int nop = fileHandle.GetNumberOfPages();
			
			L2.bp = nodepointer;
			L2.fp = L1.fp;
			L1.fp = nop;

			int y = 1;

			memcpy((char*)data,&y,sizeof(int));
			memcpy((char*)data+4,&L1,sizeof(L1));

			x = fileHandle.WritePage(nodepointer,data);

			//cout<<"NodePointer :"<<nodepointer<<endl;
			
			memcpy((char*)data,&y,sizeof(int));

			memcpy((char*)data+4,&L2,sizeof(L2));

			x = fileHandle.AppendPage(data);

			page = nop;

			newchildentry = L2.keys[0];

			//cout<<"NEW CHILD ENTRY :"<<newchildentry<<endl;

			leaf<T> L3;

			x = fileHandle.ReadPage(L2.fp,data);

			memcpy(&L3,(char*)data+4,sizeof(L3));

			L3.bp = L1.fp;

			memcpy((char*)data+4,&L3,sizeof(L3));
			
			x = fileHandle.WritePage(L2.fp,data);

			free(keys);
			free(rids);

			//cout<<"HIT!"<<endl;

			free(data);
/*
			cout<<"Key after splitting :"<<k<<endl;

			if(k > 200)
			{
				cout<<"After splitting leaf:"<<endl;

				cout<<"Leaf 1:"<<endl;
				display_leaf(L1);

				cout<<"Leaf 2:"<<endl;
				display_leaf(L2);
			}
*/			//cout<<"NEW CHILD ENTRY :"<<newchildentry<<endl;

			//display_leaf(L1);
			//display_leaf(L2);

			return 0;			
			
		}
	}	
}

RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,CompOp compOp,void *value)
{
	//cout<<"Entered OpenScan!"<<endl;

	PF_FileHandle &fileHandle = const_cast< PF_FileHandle &> ( indexHandle.fileHandle );

	//cout<<"Before!"<<endl;

	if(fileHandle.IsFree()==0)
		return 1;

	//cout<<"After!"<<endl;

	void *data = malloc(PF_PAGE_SIZE);

	int x = fileHandle.ReadPage(0,data);	

	rids.clear();
	Iterator = 0;
	state = 1;

	AttrType type;

	int root;
	memcpy(&root,(char*)data,sizeof(int));
	memcpy(&type,(char*)data+4,sizeof(type));

	free(data);	

	int vInt = -1;
	float vFloat = -1;

	//cout<<"type:"<<type<<endl;

	if(type == TypeInt)
	{
		//cout<<"TypeInt!"<<endl;

		if(value!=NULL)
			memcpy(&vInt,(char*)value,sizeof(vInt));

		return openscan(fileHandle,compOp,vInt,root);
	}
	else if(type == TypeReal)
	{
		//cout<<"TypeReal!"<<endl;

		if(value!=NULL)
			memcpy(&vFloat,(char*)value,sizeof(vFloat));
		
		return openscan(fileHandle,compOp,vFloat,root);
	}
	
	return 1;

}
template<class T>
RC IX_IndexScan::openscan(PF_FileHandle &fileHandle,CompOp compOp,T val,int root)
{

	//cout<<"Entered!"<<endl;

	//PF_FileHandle &fileHandle = const_cast< PF_FileHandle &> ( indexHandle.fileHandle );
	

	//if(fileHandle.IsFree()==0)
	//	return 1;
	
	void *data = malloc(PF_PAGE_SIZE);
	
	int flag = 1;
	int i;
	int x;

	x = fileHandle.ReadPage(root,data);
/*
	//cout<<"ROOT:"<<root<<endl;

	internals<T> Nroot;

	memcpy(&Nroot,(char*)data+4,sizeof(Nroot));

	//cout<<"Displaying root node of scan:"<<endl;

	//display_node(Nroot);

	//cout<<"Started reading!"<<endl;
	//cout<<"Before compOp:"<<endl;
*/
	switch(compOp)
	{
		case NE_OP:
				break;

		case EQ_OP :{

				memcpy(&flag,(int*)data,sizeof(int));

				while(flag == 0)
				{
					internals<T> N;

					memcpy(&N,(char*)data+4,sizeof(N));

					for(i=0;i<N.n && N.keys[i]<=val;i++);

					if(N.ptrs[i]<0)
						break;
					
					x = fileHandle.ReadPage(N.ptrs[i],data);

					memcpy(&flag,(int*)data,sizeof(int));						
				}

				leaf<T> L;

				memcpy(&L,(char*)data+4,sizeof(L));

				for(i=0;i<L.n && val!=L.keys[i];i++);

				if(val == L.keys[i])		
				{
					//intScan.push_back(val);
					rids.push_back(L.rids[i]);
				}		
				
			
			}	break;

		case LT_OP :{
				//cout<<"HI!"<<endl;

				memcpy(&flag,(int*)data,sizeof(int));
				
				while(flag == 0)
				{
					internals<T> N;

					memcpy(&N,(char*)data+4,sizeof(N));

					for(i=0;i<N.n+1 && N.ptrs[i]<0;i++);

					if(i==N.n+1)
						break;
					
					
					x= fileHandle.ReadPage(N.ptrs[i],data);

					memcpy(&flag,(int*)data,sizeof(int));

					//cout<<"HELLO"<<endl;
					
				}

				leaf<T> L;

				memcpy(&L,(char*)data+4,sizeof(L));

				int fp = 0;

				while(fp!= -1)
				{
					for(i=0;i<L.n;i++)
					{
						if(L.keys[i]<val)
						{
							//intScan.push_back(L.keys[i]);
							rids.push_back(L.rids[i]);
						}
					}

					fp = L.fp;

					if(fp!=-1)
					{
						x = fileHandle.ReadPage(fp,data);
						memcpy(&L,(char*)data+4,sizeof(L));
					}
				}				
				

				}break;
	
		case GT_OP :{
				//cout<<"HI!"<<endl;

				memcpy(&flag,(int*)data,sizeof(int));
				
				while(flag == 0)
				{
					internals<T> N;

					memcpy(&N,(char*)data+4,sizeof(N));
					/*
					cout<<"Processing following node :"<<endl;

					display_node(N);

					cout<<"------------------------------"<<endl;

					*/
					for(i=0;i<N.n+1 && N.ptrs[i]<0;i++);

					if(i==N.n+1)
						break;
					
					
					x= fileHandle.ReadPage(N.ptrs[i],data);

					memcpy(&flag,(int*)data,sizeof(int));
					
				}

				leaf<T> L;

				memcpy(&L,(char*)data+4,sizeof(L));

				/*

				cout<<"Entered the leaves at the following leaf:"<<endl;

				display_leaf(L);

				cout<<"-----------------------------------------"<<endl;
				*/
				int fp = 0;

				while(fp!= -1)
				{
					for(i=0;i<L.n;i++)
					{
						if(L.keys[i]>val)
						{
							//intScan.push_back(L.keys[i]);
							rids.push_back(L.rids[i]);
						}
					}

					fp = L.fp;

					//display_leaf(L);
					//cout<<fp<<endl;

					if(fp!=-1)
					{
						x = fileHandle.ReadPage(fp,data);
						memcpy(&L,(char*)data+4,sizeof(L));
						
						//display_leaf(L);	
					}
				}				
				

				}break;
	
		case LE_OP :
				{
				//cout<<"HI!"<<endl;

				memcpy(&flag,(int*)data,sizeof(int));
				
				internals<T> N;

				while(flag == 0)
				{
					memcpy(&N,(char*)data+4,sizeof(N));

					for(i=0;i<N.n+1 && N.ptrs[i]<0;i++);

					if(i==N.n+1)
						break;
					
					
					x= fileHandle.ReadPage(N.ptrs[i],data);

					memcpy(&flag,(int*)data,sizeof(int));
					
				}

				if(i==N.n+1)
					break;

				leaf<T> L;

				memcpy(&L,(char*)data+4,sizeof(L));

				int fp = 0;

				while(fp!= -1)
				{
					for(i=0;i<L.n;i++)
					{
						if(L.keys[i]<=val)
						{
							//intScan.push_back(L.keys[i]);
							rids.push_back(L.rids[i]);
						}
					}

					fp = L.fp;

					if(fp!=-1)
					{
						x = fileHandle.ReadPage(fp,data);
						memcpy(&L,(char*)data+4,sizeof(L));
					}
				}				
				

				}break;
	
		
		case GE_OP :
				{
				//cout<<"HI!"<<endl;

				memcpy(&flag,(int*)data,sizeof(int));
				
				while(flag == 0)
				{
					internals<T> N;

					memcpy(&N,(char*)data+4,sizeof(N));

					for(i=0;i<N.n+1 && N.ptrs[i]<0;i++);

					if(i==N.n+1)
						break;
					
					
					x= fileHandle.ReadPage(N.ptrs[i],data);

					memcpy(&flag,(int*)data,sizeof(int));
					
				}

				leaf<T> L;

				memcpy(&L,(char*)data+4,sizeof(L));

				int fp = 0;

				while(fp!= -1)
				{
					for(i=0;i<L.n;i++)
					{
						if(L.keys[i]>=val)
						{
							//intScan.push_back(L.keys[i]);
							rids.push_back(L.rids[i]);
						}
					}

					fp = L.fp;

					if(fp!=-1)
					{
						x = fileHandle.ReadPage(fp,data);
						memcpy(&L,(char*)data+4,sizeof(L));
					}
				}				
				

				}break;
	
		case NO_OP :{
				//cout<<"HI!"<<endl;

				memcpy(&flag,(int*)data,sizeof(int));
				
				while(flag == 0)
				{
					internals<T> N;

					memcpy(&N,(char*)data+4,sizeof(N));

					for(i=0;i<N.n+1 && N.ptrs[i]<0;i++);

					if(i==N.n+1)
						break;
					
					
					x= fileHandle.ReadPage(N.ptrs[i],data);

					memcpy(&flag,(int*)data,sizeof(int));
					
				}

				leaf<T> L;

				memcpy(&L,(char*)data+4,sizeof(L));

				int fp = 0;

				while(fp!= -1)
				{
					for(i=0;i<L.n;i++)
					{
						//intScan.push_back(L.keys[i]);
						rids.push_back(L.rids[i]);
					}

					fp = L.fp;

					if(fp!=-1)
					{
						x = fileHandle.ReadPage(fp,data);
						memcpy(&L,(char*)data+4,sizeof(L));
					}
				}				
				

				}break;
	}

	free(data);
	return 0;
}

RC IX_IndexScan::GetNextEntry(RID &rid)
{	

	if(Iterator == rids.size())
		return -1;
	
	rid = rids[Iterator];

	//cout<<rid.pageNum<<"????"<<rid.slotNum<<endl;


	Iterator++;
	return 0;
}           


RC IX_IndexScan::CloseScan()
{
	if(state == 1)
	{
		state=0;
		Iterator = 0;
		return 0;
	}	

	return 1;
}

RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid)
{

    void *data = malloc(PF_PAGE_SIZE);

    int x;

    x = fileHandle.ReadPage(0,data);

    int root;

    memcpy(&root,(char*)data,sizeof(int));

    int nodepointer = root;

    AttrType type;

    memcpy(&type,(char*)data+4,sizeof(type));

    int kInt;
    float kFloat;

    int flag = 0;

    if(type == TypeInt)
    {
	memcpy(&kInt,(char*)key,sizeof(kInt));
	free(data);	
	return delete1(nodepointer,kInt,rid,flag);
    }
    else if(type == TypeReal)
    {
	//cout<<"HELLO!"<<endl;
	memcpy(&kFloat,(char*)key,sizeof(kFloat));
	free(data);
	return delete1(nodepointer,kFloat,rid,flag);
    }

    free(data);

    return 1;
}
template<class T>
RC IX_IndexHandle::delete1(int nodepointer,T k,RID rid,int &flag)
{
	if(IsFree()==0)
		return 1;

    	int x;
	int old;
	int returnval;

    	void *data = malloc(PF_PAGE_SIZE);
	
    	x = fileHandle.ReadPage(nodepointer,data);

	//cout<<"Nodepointer:"<<nodepointer<<endl;

    	memcpy(&x,(char*)data,sizeof(int));
	//cout<<"deleteeeeee"<<endl;
    	if(x==0)
    	{
        	internals<T> N;

        	memcpy(&N,(char*)data+4,sizeof(N));
		//cout<<"inside x==0  "<<endl;
/*
		if(nodepointer == 1)
		{
			cout<<endl;
			display_node(N);	
			cout<<endl;
			cout<<"Trying to delete :"<<k<<endl;
		}
*/


        	int i;

        	for(i=0;i<N.n && N.keys[i]<=k;i++);

        	old = nodepointer;       

        	nodepointer = N.ptrs[i];

		//display_node(N);
		
		//cout<<"Next Node Pointer:"<<nodepointer<<endl;

        	returnval = delete1(nodepointer,k,rid,flag);

		if(returnval == 1)
		{
			free(data);
			return 1;
		}

        	if(flag == 1)
        	{
            		N.ptrs[i] = -1;

            		//void *data1 = malloc(PF_PAGE_SIZE);

            		memcpy((char*)data+4,&N,sizeof(N));

            		x = fileHandle.WritePage(old,data);           

            		flag = 0;

            		//free(data1);
        	}
    	}   

    	else
    	{
        	leaf<T> L;
       
        	memcpy(&L,(char*)data+4,sizeof(L));
		//cout<<"inside x!=0  "<<endl;
        	int i;
/*
		cout<<"Before***********************"<<endl;

		display_leaf(L);

		cout<<"***********************"<<endl;
*/
        	for(i=0;i<L.n && L.keys[i]!=k;i++);
		//cout<<"11111"<<endl;

		if(i==L.n)
		{
			free(data);
			return 1;
		}

        	for(;i<L.n-1;i++)
        	{
            		L.keys[i] = L.keys[i+1];
            		L.rids[i] = L.rids[i+1];
        	}
       		//cout<<"222222"<<endl;
        	L.keys[L.n-1]=-1;
        	L.rids[L.n-1].pageNum = 4096;
        	L.rids[L.n-1].slotNum = 4096;

        	L.n--;

        	memcpy((char*)data+4,&L,sizeof(L));
		//cout<<"333333"<<endl;
        	x = fileHandle.WritePage(nodepointer,data);

		if(L.n==0)
        	flag = -(nodepointer); 
		//cout<<"AT LAST  *** "<<endl;
/*
		cout<<"After***********************"<<endl;

		display_leaf(L);

		cout<<"***********************"<<endl;
*/
    	}


    	free(data);

	return 0;
}

IX_IndexScan::~IX_IndexScan()
{
}
