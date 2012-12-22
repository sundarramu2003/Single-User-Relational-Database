
# include "qe.h"
#include<cmath>
using namespace std;

Filter::Filter(Iterator *input,const Condition &condition)
{
	unsigned i;
	int offset,length,temp,flag,varcharcmp;
	float lhsAttr,rhsValue;

	input->getAttributes(attrs);
	count=0;
	iterator=0;

	void *data = malloc(1000);

	while(input->getNextTuple(data)!=QE_EOF)
	{
		//cout<<"Hello!"<<endl;

		offset=0;
		flag=1;

		//cout<<"I am here!"<<endl;

		for(i=0;i<attrs.size() && flag!=0;i++)	
		{

			if(attrs[i].name == condition.lhsAttr)
			{
				if(attrs[i].type == TypeVarChar)					
				{
					memcpy(&length,(char*)data+offset,sizeof(int));
					
					varcharcmp = memcmp((char*)condition.rhsValue.data,(char*)data+offset,sizeof(int)+length);

					switch(condition.op)
					{
						case EQ_OP: flag=(varcharcmp == 0);break;
						case LT_OP: flag=(varcharcmp<0);break;
						case GT_OP: flag=(varcharcmp>0);break;
						case LE_OP: flag=(varcharcmp<=0);break;
						case GE_OP: flag=(varcharcmp>=0);break;
						case NE_OP: flag=(varcharcmp!=0);break;
						case NO_OP: flag=1; 
					}

					offset+=sizeof(int)+length;
					
				}
				else
				{ 
					if(condition.rhsValue.type == TypeInt)
					{
						memcpy(&temp,(char*)(condition.rhsValue.data),sizeof(int));
						rhsValue = temp;

						memcpy(&temp,(char*)data+offset,sizeof(int));
						lhsAttr = temp;
					}		
					else
					{
						memcpy(&rhsValue,(char*)(condition.rhsValue.data),sizeof(float));
						memcpy(&lhsAttr,(char*)data+offset,sizeof(float));
					}
					switch(condition.op)
					{
						case EQ_OP: flag=(lhsAttr == rhsValue);break;
						case LT_OP: flag=(lhsAttr<rhsValue);break;
						case GT_OP: flag=(lhsAttr>rhsValue);break;
						case LE_OP: flag=(lhsAttr<=rhsValue);break;
						case GE_OP: flag=(lhsAttr>=rhsValue);break;
						case NE_OP: flag=(lhsAttr!=rhsValue);break;
						case NO_OP: flag=1; 
					}
					
					offset+=sizeof(int);	//sizeof(int)=sizeof(real)

				}
			}
			else
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data+offset,sizeof(int));
					offset+=sizeof(int)+length;	
				}
				else
				{
					offset+=sizeof(int); //sizeof(int)=sizeof(real)
				}
			}
		}

		if(flag==1)
		{
			void *temp = malloc(offset);
			memcpy((char*)temp,(const char*)data,offset);
			dataselect.push_back(temp);
			len.push_back(offset);
			count++;
		}
	}	

	free(data);
}

Filter::~Filter()
{
	for( std::vector< void * > ::iterator iter = dataselect.begin(); iter != dataselect.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}
}

RC Filter::getNextTuple(void *data)
{
	if(iterator == count)
		return QE_EOF;

	memcpy(data,this->dataselect[iterator],len[iterator]);
	iterator++;

	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

Project::Project(Iterator *input,const vector<string> &attrNames)
{
	vector<Attribute> attrs;
	unsigned i,j;
	int offset1,flag,offset2,length;
	void *data,*temp;


	int x;

	input->getAttributes(attrs);
	this->iterator=0;
	this->count=0;

	for(i=0;i<attrs.size();i++)
	{
		for(j=0;j<attrNames.size();j++)
		{
			if(attrs[i].name == attrNames[j])
				this->attrs.push_back(attrs[i]);
		}
	}



	data = malloc(100);


	while(input->getNextTuple(data)!=QE_EOF)
	{
		offset1=0;
		offset2=0;

		temp = malloc(100);

		for(i=0;i<attrs.size();i++)
		{
			flag=0;

			for(j=0;j<attrNames.size() && flag==0;j++)
			{
				if(attrs[i].name == attrNames[j])
					flag=1;
			}

			if(flag==1)
			{
					if(attrs[i].type == TypeVarChar)
					{
						//cout<<"WRONG!"<<endl;

						memcpy(&length,(char*)data+offset1,sizeof(int));
						memcpy((char*)temp+offset2,&length,sizeof(int));

						offset1+=sizeof(int);
						offset2+=sizeof(int);

						memcpy((char*)temp+offset2,(char*)data+offset1,length);
						
						offset1+=length;
						offset2+=length;
					}
					else
					{
						
						memcpy((char*)temp+offset2,(char*)data+offset1,sizeof(int));
						
						memcpy(&x,(char*)temp+offset2,sizeof(int));

						//cout<<x<<endl;

						offset1+=sizeof(int);	//sizeof(int)=sizeof(real)
						offset2+=sizeof(int);	//sizeof(int)=sizeof(real)
	
						//cout<<"RIGHT"<<"\t"<<offset1<<"\t"<<offset2<<endl;

					}
			}
			else
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data+offset1,sizeof(int));
					offset1+=sizeof(int)+length;
				}
				else
				{
					offset1+=sizeof(int);	//sizeof(int)=sizeof(real)
				}
			}

		}

		dataproject.push_back(temp);
		len.push_back(offset2);
		count++;
	}

	free(data);	
}          

Project::~Project()
{
	for( std::vector< void * > ::iterator iter = dataproject.begin(); iter != dataproject.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}
	
}

RC Project::getNextTuple(void *data)
{
	if(iterator==count)
		return QE_EOF;

	memcpy((char*)data,(char*)this->dataproject[iterator],this->len[iterator]);
	iterator++;

	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

NLJoin::NLJoin(Iterator *leftIn,TableScan *rightIn,const Condition &condition,const unsigned numPages)
{
	vector<Attribute> attrs1;
	unsigned size1,size2,i;
	int length,offset1,jlength1,offset2,jlength2,n,tmp,res;
	AttrType type1,type2;
	float x,y;

	leftIn->getAttributes(attrs);
	size1 = attrs.size();
	rightIn->getAttributes(attrs1);
	size2 = attrs1.size();

	attrs.insert(attrs.end(),attrs1.begin(),attrs1.end());
	iterator=0;
	count=0;
	

	void *data1 = malloc(500);
	void *data2 = malloc(500);
	void *temp1 = malloc(500);
	void *temp2 = malloc(500);

	while(leftIn->getNextTuple(data1)!=QE_EOF)
	{
		offset1=0;

		for(i=0;i<size1;i++)
		{
			if(attrs[i].name == condition.lhsAttr)
			{
				type1 = attrs[i].type;

				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data1+offset1,sizeof(int));
					memcpy((char*)temp1,(char*)data1+offset1,sizeof(int)+length);
					jlength1 = length;	
				}
				else
				{
					memcpy((char*)temp1,(char*)data1+offset1,sizeof(int));	//sizeof(int)=sizeof(real)
					//jlength1 = sizeof(int);				//sizeof(int)=sizeof(real)
				}
			}
			
			if(attrs[i].type == TypeVarChar)
			{
				memcpy(&length,(char*)data1+offset1,sizeof(int));
				offset1+=sizeof(int)+length;
			}
			else
			{
				offset1+=sizeof(int);	//sizeof(int)=sizeof(real)
			}
				
		}

		rightIn->setIterator();

		while(rightIn->getNextTuple(data2)!=QE_EOF)
		{
			offset2=0;

			for(i=0;i<size2;i++)
			{
				if(attrs1[i].name == condition.rhsAttr)
				{
					type2 = attrs1[i].type;					

					if(attrs1[i].type == TypeVarChar)
					{
						memcpy(&length,(char*)data2+offset2,sizeof(int));
						memcpy((char*)temp2,(char*)data2+offset2,sizeof(int)+length);
						jlength2 = length;	
					}
					else
					{
						memcpy((char*)temp2,(char*)data2+offset2,sizeof(int));	//sizeof(int)=sizeof(real)
						//jlength2 = sizeof(int);					//sizeof(int)=sizeof(real)
					}
				}	
			
				if(attrs1[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data2+offset2,sizeof(int));
					offset2+=sizeof(int)+length;
				}
				else
				{
					offset2+=sizeof(int);	//sizeof(int)=sizeof(real)
				}
			}
			
			if(type1 == TypeVarChar)
			{	
				char *s1 = (char*)malloc(jlength1+1);
				char *s2 = (char*)malloc(jlength2+1);

				memcpy((char*)s1,(char*)temp1+sizeof(int),jlength1);
				memcpy((char*)s2,(char*)temp2+sizeof(int),jlength2);

				s1[jlength1]='\0';
				s2[jlength2]='\0';

				n = strcmp(s1,s2);

				switch(condition.op)
				{
					case EQ_OP:res=(n==0);break;
					case LT_OP:res=(n<0);break;
					case GT_OP:res=(n>0);break;
					case LE_OP:res=(n<=0);break;
					case GE_OP:res=(n>=0);break;
					case NE_OP:res=(n!=0);break;
					case NO_OP:res=1;
				}

				free(s1);
				free(s2);
			}
			else
			{
				if(type1 == TypeInt)
				{
					memcpy(&tmp,(char*)temp1,sizeof(int));
					x = (float)tmp;
					memcpy(&tmp,(char*)temp2,sizeof(int));
					y = (float)tmp;
				}
				else
				{
					memcpy(&x,(char*)temp1,sizeof(float));
					memcpy(&y,(char*)temp2,sizeof(float));	
				}

				switch(condition.op)
				{
					case EQ_OP:res=(x==y);break;
					case LT_OP:res=(x<y);break;
					case GT_OP:res=(x>y);break;
					case LE_OP:res=(x<=y);break;
					case GE_OP:res=(x>=y);break;
					case NE_OP:res=(x!=y);break;
					case NO_OP:res=1;
				}				
			}

			if(res==1)
			{
				void *temp = malloc(offset1+offset2);
				
				memcpy((char*)temp,(char*)data1,offset1);
				memcpy((char*)temp+offset1,(char*)data2,offset2);
				
				datanljoin.push_back(temp);
				len.push_back(offset1+offset2);
				count++;
			}
		}
	}

	free(data1);
	free(data2);
	free(temp1);
	free(temp2);			
}

NLJoin::~NLJoin()
{
	for( std::vector< void * > ::iterator iter = datanljoin.begin(); iter != datanljoin.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}	
}

RC NLJoin::getNextTuple(void *data)
{
	if(iterator==count)
		return QE_EOF;

	memcpy((char*)data,this->datanljoin[iterator],this->len[iterator]);
	iterator++;

	return 0;
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();

	attrs = this->attrs;
}

INLJoin::INLJoin(Iterator *leftIn,IndexScan *rightIn,const Condition &condition,const unsigned numPages)
{
	vector<Attribute> attrs1;
	unsigned size1,size2,i;
	int length,offset1,jlength1,offset2,n;
	CompOp compOp;		

	leftIn->getAttributes(attrs);
	size1 = attrs.size();
	rightIn->getAttributes(attrs1);
	size2 = attrs1.size();

	attrs.insert(attrs.end(),attrs1.begin(),attrs1.end());
	iterator = 0;
	count = 0;

	void *data1 = malloc(500);
	void *data2 = malloc(500);
	void *temp1 = malloc(500);


	while(leftIn->getNextTuple(data1)!=QE_EOF)
	{
		offset1=0;

		for(i=0;i<size1;i++)
		{
			if(attrs[i].name == condition.lhsAttr)
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data1+offset1,sizeof(int));
					memcpy((char*)temp1,(char*)data1+offset1,sizeof(int)+length);
					jlength1 = length;	
				}
				else
				{
					memcpy((char*)temp1,(char*)data1+offset1,sizeof(int));	//sizeof(int)=sizeof(real)
					//jlength1 = sizeof(int);				//sizeof(int)=sizeof(real)
				}
			}
			
			if(attrs[i].type == TypeVarChar)
			{
				memcpy(&length,(char*)data1+offset1,sizeof(int));
				offset1+=sizeof(int)+length;
			}
			else
			{
				offset1+=sizeof(int);	//sizeof(int)=sizeof(real)
			}
				
		}

		switch(condition.op)
		{
			case EQ_OP:;
			case NE_OP:;
			case NO_OP:compOp = condition.op;break;
			case LT_OP:compOp = GT_OP;break;
			case GT_OP:compOp = LT_OP;break;
			case LE_OP:compOp = GE_OP;break;
			case GE_OP:compOp = LE_OP;
		}

		rightIn->setIterator(compOp,temp1);

		while(rightIn->getNextTuple(data2)!=QE_EOF)
		{
			offset2=0;

			for(i=0;i<size2;i++)
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data2+offset2,sizeof(int));
					offset2+=sizeof(int)+length;					
				}	
				else
				{
					offset2+=sizeof(int);
				}
			}

			void *temp = malloc(offset1+offset2);
			memcpy((char*)temp,(char*)data1,offset1);
			memcpy((char*)temp+offset1,(char*)data2,offset2);


			datainljoin.push_back(temp);
			len.push_back(offset1+offset2);
			count++;
		}
				
	}
		

	free(data1);
	free(data2);
	free(temp1);
			
}

INLJoin::~INLJoin()
{
	for( std::vector< void * > ::iterator iter = datainljoin.begin(); iter != datainljoin.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}	

}

RC INLJoin::getNextTuple(void *data)
{
	if(iterator==count)
		return QE_EOF;

	memcpy((char*)data,this->datainljoin[iterator],this->len[iterator]);
	iterator++;

	return 0;	
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}



HashJoin::HashJoin(Iterator *leftIn,Iterator *rightIn,const Condition &condition,unsigned numPages)
{
	vector<Attribute> attrs1;
	unsigned size1,size2,i,k;
	void **partitions;
	int *offsets,offset1,ihash,ikey,length,offset3,offset5,ikey2;
	float fhash,fkey,fkey2;
	
	numPages = numPages*100;

	offsets = (int*)malloc(numPages*sizeof(int)); 
	partitions = (void**)malloc(numPages*sizeof(void*));

	for(i=0;i<numPages;i++)
	{
		partitions[i] = malloc(PF_PAGE_SIZE);
		offsets[i] = 0;
		memcpy((char*)partitions[i],&offsets[i],sizeof(int));
	}

	//cout<<"HELLO!"<<endl;

	leftIn->getAttributes(attrs);
	size1 = attrs.size();
	rightIn->getAttributes(attrs1);
	size2 = attrs1.size();

	attrs.insert(attrs.end(),attrs1.begin(),attrs1.end());
	iterator =0;
	count = 0;

	void *data1 = malloc(500);
	
	while(rightIn->getNextTuple(data1)!=QE_EOF)
	{
		offset1=0;

		cout<<"HELLO!"<<endl;		

		for(i=0;i<size2;i++)
		{
			if(attrs1[i].name == condition.rhsAttr)
			{
				if(attrs1[i].type == TypeVarChar)
				{
					char c;

					memcpy(&length,(char*)data1+offset1,sizeof(int));
					offset1+=sizeof(int);
					memcpy(&c,(char*)data1+offset1,sizeof(char));
					ihash = ((int)c)%(numPages);
					offset1+=length;
					
				}
				if(attrs1[i].type == TypeReal)
				{
					memcpy(&fkey,(char*)data1+offset1,sizeof(float));
					fhash = fmod(fkey,numPages);
					ihash = (int)fhash;
					offset1+=sizeof(int);	//sizeof(int)=sizeof(real)
				}
				else if(attrs1[i].type == TypeInt)
				{
					memcpy(&ikey,(char*)data1+offset1,sizeof(float));
					ihash = ikey%(numPages);
					offset1+=sizeof(int);	//sizeof(int)=sizeof(real)
				}

					
			}
			else
			{
				if(attrs1[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data1+offset1,sizeof(int));
					offset1+=sizeof(int)+length;
				}
				else
				{
					offset1+=sizeof(int);
				}
			}
		}

		memcpy((char*)partitions[ihash]+offsets[ihash],(char*)data1,offset1);
		
		offsets[ihash]+=offset1;		
	}	

//	cout<<"HELLO!"<<endl;


	while(leftIn->getNextTuple(data1)!=QE_EOF)
	{
		offset1=0;

		for(i=0;i<size1;i++)
		{
			if(attrs[i].name == condition.lhsAttr)
			{
				if(attrs[i].type == TypeReal)
				{
					memcpy(&fkey,(char*)data1+offset1,sizeof(float));
					fhash = fmod(fkey,numPages);
					ihash = (int)fhash;
				}
				else if(attrs[i].type == TypeInt)
				{
					memcpy(&ikey,(char*)data1+offset1,sizeof(float));
					ihash = ikey%(numPages);
					fkey = ikey;
				}

				offset1+=sizeof(int);	//sizeof(int)=sizeof(real)	
			}
			else
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data1+offset1,sizeof(int));
					offset1+=sizeof(int)+length;
				}
				else
				{
					offset1+=sizeof(int);
				}
			}
		}

		
		offset3 = 0;
		
		while(offset3<offsets[ihash])
		{
			offset5 = offset3;

			for(k=0;k<size2;k++)
			{
				if(attrs1[k].name == condition.rhsAttr)
				{
					if(attrs1[k].type == TypeInt)
					{
						memcpy(&ikey2,(char*)partitions[ihash]+offset3,sizeof(int));
						fkey2 = (float)ikey2;
					}
					else
					{
						memcpy(&fkey2,(char*)partitions[ihash]+offset3,sizeof(float));
					}

					offset3+=sizeof(int);
				}
				else
				{
					if(attrs1[k].type == TypeVarChar)
					{
						memcpy(&length,(char*)partitions[ihash]+offset3,sizeof(int));
						offset3+=sizeof(int)+length;
					}
					else
					{
						offset3+=sizeof(int);
					}
				}	
			}

			if(fkey == fkey2)
			{
				void *temp = malloc(offset1+offset3-offset5);

				memcpy((char*)temp,(char*)data1,offset1);
				memcpy((char*)temp+offset1,(char*)partitions[ihash]+offset5,offset3-offset5);

				datahashjoin.push_back(temp);
				len.push_back(offset1+offset3-offset5);
				count++;
			}			
		}		
		
	}


	for(i=0;i<numPages;i++)
	{
		free(partitions[i]);
	}
	
	free(partitions);	
	free(offsets);
	free(data1);
}

HashJoin::~HashJoin()
{
	for( std::vector< void * > ::iterator iter = datahashjoin.begin(); iter != datahashjoin.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}	
}

RC HashJoin::getNextTuple(void *data)
{
	if(iterator==count)
		return QE_EOF;

	memcpy((char*)data,(char*)this->datahashjoin[iterator],this->len[iterator]);
	iterator++;
	return 0;
}
        

void HashJoin::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();

	attrs = this->attrs;
}

Aggregate::Aggregate(Iterator *input,Attribute aggAttr,AggregateOp op)
{
	unsigned i;

	vector<Attribute> attrs;

	input->getAttributes(attrs);

	void *data = malloc(500);

	float fmin=0.0,fmax=0.0,fsum=0.0,favg=0.0,ftmp,aggcount=0;
	int itmp,tmp;

	iterator =0;
	count=0;


	int offset;
				
	while(input->getNextTuple(data)!=QE_EOF)
	{
		offset=0;

		for(i=0;i<attrs.size();i++)
		{
			if(attrs[i].name == aggAttr.name)
			{
				if(aggAttr.type==TypeInt)
				{
                                	memcpy(&itmp,(char*)data+offset,sizeof(int));
					ftmp = (float)(itmp);								
				}
				else if(aggAttr.type==TypeReal)
				{
                                	memcpy(&ftmp,(char*)data+offset,sizeof(int));
				}

				fmin = (fmin<ftmp)? fmin:ftmp;
				fmax = (fmax>ftmp)? fmax:ftmp;
				fsum+=ftmp;
				aggcount++;					
			
				offset+=sizeof(int);
			}
			else
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&tmp,(char*)data+offset,sizeof(int));
					offset+=sizeof(int)+tmp;
				}
				else
					offset+=sizeof(int);
			}
		}
	}

	favg = fsum/aggcount;

	void *temp = malloc(4);

	switch(op)
		{
			case MIN: memcpy((char*)temp,&fmin,sizeof(float));break;
			case MAX: memcpy((char*)temp,&fmax,sizeof(float));break;
			case SUM: memcpy((char*)temp,&fsum,sizeof(float));break;
			case AVG: memcpy((char*)temp,&favg,sizeof(float));break;
			case COUNT: memcpy((char*)temp,&aggcount,sizeof(float));
		}

	dataagg.push_back(temp);
	len.push_back(4);
	count++;

	string name;

	Attribute dummy;

	switch(op)
	{
		case MIN: name = "MIN";break;
		case MAX: name = "MAX";break;
		case SUM: name = "SUM";break;
		case AVG: name = "AVG";break;
		case COUNT: name = "COUNT";
	}

	name+="("+aggAttr.name+")";

	dummy.name = name;

	switch(op)
	{
		case MIN:;
		case MAX:;
		case SUM:dummy.type = aggAttr.type;break;
		case COUNT: dummy.type = TypeInt;break;
		case AVG: dummy.type = TypeReal;
	}

	dummy.length = 4;

	this->attrs.push_back(dummy);

	free(data);
}

Aggregate::~Aggregate()
{
	for( std::vector< void * > ::iterator iter = dataagg.begin(); iter != dataagg.end(); ++iter ) 
	{
		free( ( void * )( *iter ) ); 
	}		
}

RC Aggregate::getNextTuple(void *data)
{
	if(iterator==count)
		return QE_EOF;

	memcpy((char*)data,(char*)this->dataagg[iterator],this->len[iterator]);
	iterator++;
	return 0;	
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();

	attrs = this->attrs;
}

struct running_info
{
	float fmin,fmax,fcount,fsum,favg;

	running_info()
	{
		fmin = fmax = fcount = fsum = favg = 0.0;
	}
};


float min(float a,float b)
{
	if(a<b)
		return a;

	return b;
}

float max(float a,float b)
{
	if(a>b)
		return a;

	return b;
}

template<class T>
void process(vector<T> &gVal,vector<running_info> &gInfo,T gvalue,float value)
{
	unsigned i;
	int flag=0;

	
	for(i=0;i<gVal.size() && flag==0;i++)
	{
		if(gVal[i]==gvalue)
			flag=1;
	}

	if(flag==0)
	{
		gVal.push_back(gvalue);
		running_info r;
		
		r.fmin = value;
		r.fmax = value;

		gInfo.push_back(r);
	}
	else
	{
		i--;
	}

	gInfo[i].fmin = min(gInfo[i].fmin,value);
	gInfo[i].fmax = min(gInfo[i].fmax,value);
	gInfo[i].fsum+=value;
	gInfo[i].fcount++;
	gInfo[i].favg = gInfo[i].fsum/gInfo[i].fcount;
}

Aggregate::Aggregate(Iterator *input,Attribute aggAttr,Attribute gAttr,AggregateOp op)
{
	int offset,length;
	unsigned i;
	vector<string> gVal1;
	vector<float> gVal2;
	vector<running_info> gInfo;
	float value;
	string gvalue1;	
	float gvalue2;

	vector<Attribute> attrs;

	iterator=0;
	count=0;

	input->getAttributes(attrs);

	void *data = malloc(500);

	while(input->getNextTuple(data)!=QE_EOF)	
	{
		offset = 0;

		for(i=0;i<attrs.size();i++)
		{
			if(attrs[i].name == aggAttr.name)
			{
				if(attrs[i].type == TypeReal)
				{
					memcpy(&value,(char*)data+offset,sizeof(float));
				}
				else
				{
					int x;
					memcpy(&x,(char*)data+offset,sizeof(float));
					value = (int)x;
				}

				//cout<<"Caught aggAttr:"<<value<<endl;
			}

			if(attrs[i].name == gAttr.name)
			{
				if(attrs[i].type == TypeVarChar)
				{
					memcpy(&length,(char*)data+offset,sizeof(int));
					char *str =(char*) malloc(length+1);

					memcpy((char*)str,(char*)data+offset+sizeof(int),length);

					str[length]='\0';

					gvalue1 = str;

					free(str);	
				}
				else
				{
					if(attrs[i].type == TypeReal)
					{
						memcpy(&gvalue2,(char*)data+offset,sizeof(int));
					}
					else
					{
						int x;
						memcpy(&x,(char*)data+offset,sizeof(int));
						gvalue2 = (float)x;
					}
					//cout<<"Caught gAttr:"<<gvalue2<<endl;	
				}
			}
		
			if(attrs[i].type == TypeVarChar)
			{
				memcpy(&length,(char*)data+offset,sizeof(int));
				offset+=sizeof(int)+length;
			}
			else
			{
				offset+=sizeof(int);
			}
		}

		if(gAttr.type == TypeVarChar)
			process(gVal1,gInfo,gvalue1,value);
		else
			process(gVal2,gInfo,gvalue2,value);
	
	}

	for(i=0;i<gInfo.size();i++)
	{
		void *temp = malloc(200);
		offset =0;

		if(gAttr.type == TypeVarChar)
		{
			length = gVal1[i].length();
			memcpy((char*)temp+offset,&length,sizeof(int));
			offset+=sizeof(int);
			memcpy((char*)temp+offset,gVal1[i].c_str(),length);
			offset+=length;
		}
		else
		{
			memcpy((char*)temp+offset,&gVal2[i],sizeof(float));
			offset+=sizeof(float);
		}

		switch(op)
		{
			case MIN: memcpy((char*)temp+offset,&gInfo[i].fmin,sizeof(float));break;
			case MAX: memcpy((char*)temp+offset,&gInfo[i].fmax,sizeof(float));break;
			case AVG: memcpy((char*)temp+offset,&gInfo[i].favg,sizeof(float));break;
			case SUM: memcpy((char*)temp+offset,&gInfo[i].fsum,sizeof(float));break;
			case COUNT: memcpy((char*)temp+offset,&gInfo[i].fcount,sizeof(float));		
		}

		offset+=sizeof(float);

		dataagg.push_back(temp);
		len.push_back(offset);
		count++;
	}

	this->attrs.push_back(gAttr);

	string name;

	Attribute dummy;

	switch(op)
	{
		case MIN: name = "MIN";break;
		case MAX: name = "MAX";break;
		case SUM: name = "SUM";break;
		case AVG: name = "AVG";break;
		case COUNT: name = "COUNT";
	}

	name+="("+aggAttr.name+")";

	dummy.name = name;

	switch(op)
	{
		case MIN:;
		case MAX:;
		case SUM:dummy.type = aggAttr.type;break;
		case COUNT: dummy.type = TypeInt;break;
		case AVG: dummy.type = TypeReal;
	}

	dummy.length = 4;

	this->attrs.push_back(dummy);



	free(data);
}     

