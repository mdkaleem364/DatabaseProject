/* testpf.c */
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include "pf.h"
#include "pftypes.h"
#include <string.h>
#include <time.h>
#include <stdbool.h>

#define FILE1	"file1"
#define FILE2	"file2"

#define LOW_LIMIT(salary) salary<500?1:0
#define MID_LIMIT(salary) (salary>=500&&salary<1000)?1:0
#define HIGH_LIMIT(salary) salary>=1000?1:0
#define TESTING_DATA_FILE_NAME "testingData"
#define RELATION_NAME "Jan2015Transaction"
#define MAX_SELECT_OPTION 4
#define MAX_COMPARISION_OPTION 3
#define IS_VALID_GENDER(gender) (gender==1||gender==2)?true:false
#define IS_VALID_MARITAL_STATUS(status) (status==1||status==2)?true:false
#define SALARY_TYPE(amount) amount<500?"SALARY='Low'":amount<1000?"SALARY='Medium'":"SALARY='High'"
#define GET_BIT_MAP_FOR_SALARY(amount) amount<500?16:amount<1000?8:4
#define MAX_PAGE_SIZE 4090

static int totalNoOfData=0;

void sequentialWriteIndex2(int fd,int data,int *currentPageNo){

	int *buf;
	int error,pagenum;

	char dataToWrite[100];
	if(*currentPageNo<0){
		if ((error=PF_AllocPage(fd,&pagenum,(char **)&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		*currentPageNo = pagenum;
		sprintf(dataToWrite,"%d",data);
		strcpy(buf,dataToWrite);
		//unfix as it is sequential write
		if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
			PF_PrintError("unfix buffer\n");
			exit(1);
		}

	}else{
		pagenum=*currentPageNo-1;
		if((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
			*currentPageNo = pagenum;
			int sizeOfCurrentPage=strlen(buf);
			if(sizeOfCurrentPage<MAX_PAGE_SIZE){
				sprintf(dataToWrite,",%d",data);
				strcat(buf,dataToWrite);
				//unfix as it is sequential write
				if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
					PF_PrintError("unfix buffer\n");
					exit(1);
				}
			}else{
				if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
					PF_PrintError("unfix buffer\n");
					exit(1);
				}
				if ((error=PF_AllocPage(fd,&pagenum,(char **)&buf))!= PFE_OK){
					PF_PrintError("first buffer\n");
					exit(1);
				}
				*currentPageNo = pagenum;
				sprintf(dataToWrite,"%d",data);
				strcpy(buf,dataToWrite);
				//unfix as it is sequential write
				if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
					PF_PrintError("unfix buffer\n");
					exit(1);
				}
			}
		}
	}


}
void sequentialWriteIndex(int fd,int data,int* hashArray,int pageNumber){
	int *buf;
	int error,pagenum;
	char updateIndexArray[100];
	char dataToWrite[100],pageNumberStr[20];
	if(hashArray[data]<0){
		if ((error=PF_AllocPage(fd,&pagenum,(char **)&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		hashArray[data] = pagenum;
//		if(data==74){TODO resolve essuee of 74
//			pageNumber=pageNumber+1;
//		}
		sprintf(dataToWrite,"%d,%d",data,pageNumber);
		//*((int *)buf) = data;
		strcpy(buf,dataToWrite);
		//unfix as it is sequential write
		if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
			PF_PrintError("unfix buffer\n");
			exit(1);
		}
	}
	else{
		pagenum = hashArray[data]-1;
		if((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
			//strcpy(updateIndexArray,buf);
			int sizeofHashPage=strlen(buf);
			if(sizeofHashPage>4000){
				//Allocate ne page and store data in it
				if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
					PF_PrintError("unfix");
					exit(1);
				}
				if ((error=PF_AllocPage(fd,&pagenum,(char **)&buf))!= PFE_OK){
					PF_PrintError("first buffer\n");
					exit(1);
				}
				hashArray[data] = pagenum;
				sprintf(dataToWrite,"%d,%d",data,pageNumber);
				//*((int *)buf) = data;
				strcpy(buf,dataToWrite);
				//unfix as it is sequential write
				if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
					PF_PrintError("unfix buffer\n");
					exit(1);
				}
			}else{
				sprintf(pageNumberStr,"%d",pageNumber);
				strcat(buf,",");
				strcat(buf,pageNumberStr);
				if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
					PF_PrintError("unfix");
					exit(1);
				}
			}

		}
	}

	/*while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
			printf("got page %d, %d\n",pagenum,*buf);
			if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
				PF_PrintError("unfix");
				exit(1);
			}
		}*/

}

int sequentialWriteInternalData(int fd,char* data){
	int *buf;
	int error,pagenum;
	if ((error=PF_AllocPage(fd,&pagenum,(char **)&buf))!= PFE_OK){
		PF_PrintError("first buffer\n");
		exit(1);
	}
	//TODO remove this comment
//	printf("data written to memory %s \n",data);
	strcpy(buf,data);
	//unfix as it is sequential write
	if ((error=PF_UnfixPage(fd,pagenum,TRUE))!= PFE_OK){
		PF_PrintError("unfix buffer\n");
		exit(1);
	}
	return pagenum;
}

int extractDataFromInputString(char * input){
	int result=-1;
	int data=0;
	char* dataTokensToRead ;
	dataTokensToRead = strtok(input,",");
	dataTokensToRead = strtok(NULL,",");
	dataTokensToRead = strtok(NULL,",");
	dataTokensToRead = strtok(NULL,",");
	dataTokensToRead = strtok(NULL,",");
//	dataTokensToRead = strtok(NULL,",");
	if(toupper(dataTokensToRead[0])=='M'){
		//Male is at sixth bit in bitmap representation.
			data+=64;
		}else if(toupper(dataTokensToRead[0])=='F'){
			//Female is at fifth bit in bitmap representation.
			data+=32;
		}else{
			printf("Error in data\n");
			return result;
	}
	dataTokensToRead = strtok(NULL,",");
	int intSalary=atoi(dataTokensToRead);
	if(LOW_LIMIT(intSalary)){
		data+=16;
	}else if(MID_LIMIT(intSalary)){
		data+=8;
	}else if(HIGH_LIMIT(intSalary)){
		data+=4;
	}
	dataTokensToRead = strtok(NULL,",");


	if(toupper(dataTokensToRead[0])=='S' ){
		data+=2;
	}else if(toupper(dataTokensToRead[0])=='M'){
		data+=1;
	}
		//If it reaches here means data format is correct
	result=data;
	return result;

}
bool sequentialWrite2(char *fname1,char * fname2){
	bool writeStatus=false;
	int fd1,fd2,error,pageNumber,currentPageNo=-1;
	char input[100],dataInput[100];

	FILE* fp;
	int dataToBeStored=0;

	//extractDataFromInputString(input1);
	if ((fd1=PF_OpenFile(fname1))<0){
		PF_PrintError("open file1");
		exit(1);
	}

	if ((fd2=PF_OpenFile(fname2))<0){
		PF_PrintError("open file2");
		exit(1);
	}

	fp = fopen(TESTING_DATA_FILE_NAME,"r");
	if(!fp){
		printf("file '%s' not found .",TESTING_DATA_FILE_NAME);
		exit(1);
	}
	while(fgets(input,100,fp)){
//		printf("%s",input);
		strcpy(dataInput,input);
		dataToBeStored=extractDataFromInputString(input);
//		printf("%s",dataInput);
		if(dataToBeStored>=0){
			sequentialWriteInternalData(fd2,dataInput);
			sequentialWriteIndex2(fd1,dataToBeStored,&currentPageNo);

		}
		memset(&input,0,sizeof(input));
	}
	fclose(fp);

	if ((error=PF_CloseFile(fd1))!= PFE_OK){
		PF_PrintError("close file1\n");
		exit(1);
	}

	if ((error=PF_CloseFile(fd2))!= PFE_OK){
		PF_PrintError("close file2\n");
		exit(1);
	}
	return writeStatus;
}
bool sequentialWrite(char *fname1,char * fname2,int* hashArray){
	bool writeStatus=false;
	int fd1,fd2,error,pageNumber,currentPageNo=-1;
	char input[100],dataInput[100];

	FILE* fp;
	int dataToBeStored=0;

	//extractDataFromInputString(input1);
	if ((fd1=PF_OpenFile(fname1))<0){
		PF_PrintError("open file1");
		exit(1);
	}

	if ((fd2=PF_OpenFile(fname2))<0){
		PF_PrintError("open file2");
		exit(1);
	}

	fp = fopen(TESTING_DATA_FILE_NAME,"r");
	if(!fp){
		printf("file '%s' not found .",TESTING_DATA_FILE_NAME);
		exit(1);
	}
	while(fgets(input,100,fp)){
//		printf("%s",input);
		strcpy(dataInput,input);
		dataToBeStored=extractDataFromInputString(input);
//		printf("%s",dataInput);
		if(dataToBeStored>=0){
			pageNumber = sequentialWriteInternalData(fd2,dataInput);
			//totalNoOfData is a global vriable
			totalNoOfData++;
			sequentialWriteIndex2(fd1,dataToBeStored,&currentPageNo);

		}
		memset(&input,0,sizeof(input));
	}
	fclose(fp);

	if ((error=PF_CloseFile(fd1))!= PFE_OK){
		PF_PrintError("close file1\n");
		exit(1);
	}

	if ((error=PF_CloseFile(fd2))!= PFE_OK){
		PF_PrintError("close file2\n");
		exit(1);
	}
	return writeStatus;
}

/************************************************************
Open the File.
allocate as many pages in the file as the buffer
manager would allow, and write the page number
into the data.
then, close file.
******************************************************************/
writefile(fname)
char *fname;
{
int i;
int fd,pagenum;
int *buf;
int error;

	/* open file1, and allocate a few pages in there */
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file1");
		exit(1);
	}
	printf("opened %s\n",fname);

	for (i=0; i < PF_MAX_BUFS; i++){
		if ((error=PF_AllocPage(fd,&pagenum,&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		*((int *)buf) = i;
		printf("allocated page %d\n",pagenum);
	}

	if ((error=PF_AllocPage(fd,&pagenum,&buf))==PFE_OK){
		printf("too many buffers, and it's still OK\n");
		exit(1);
	}

	/* unfix these pages */
	for (i=0; i < PF_MAX_BUFS; i++){
		if ((error=PF_UnfixPage(fd,i,TRUE))!= PFE_OK){
			PF_PrintError("unfix buffer\n");
			exit(1);
		}
	}
	int j;
	for (j=i; j< PF_MAX_BUFS+PF_MAX_BUFS;j++){
			if ((error=PF_AllocPage(fd,&pagenum,&buf))!= PFE_OK){
				PF_PrintError("first buffer\n");
				exit(1);
			}
			*((int *)buf) = j;
			printf("allocated page %d\n",pagenum);
		}
	/* unfix these pages */
	for (i=PF_MAX_BUFS; i < PF_MAX_BUFS*2; i++){
		if ((error=PF_UnfixPage(fd,i,TRUE))!= PFE_OK){
			PF_PrintError("unfix buffer\n");
			exit(1);
		}
	}



	/* close the file */
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file1\n");
		exit(1);
	}

}

/**************************************************************
print the content of file
*************************************************************/
readfile(fname)
char *fname;
{
int error;
int *buf;
int pagenum;
int fd;

	printf("opening %s\n",fname);
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file");
		exit(1);
	}
	printfile(fd);
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file");
		exit(1);
	}
}
readfileData(fname)
char *fname;
{
int error;
int *buf;
int pagenum;
int fd;

	printf("opening %s\n",fname);
	if ((fd=PF_OpenFile(fname))<0){
		PF_PrintError("open file");
		exit(1);
	}
	printfileData(fd);
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file");
		exit(1);
	}
}

printfile(fd)
int fd;
{
int error;
int *buf;
int pagenum;

	printf("reading file\n");
	pagenum = -1;
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
		printf("got page %d, %d\n",pagenum,*buf);
		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
	}
	if (error != PFE_EOF){
		PF_PrintError("not eof\n");
		exit(1);
	}
	printf("eof reached\n");

}
printfileData(fd)
int fd;
{
int error;
int *buf;
int pagenum;

	printf("reading file\n");
	pagenum = -1;
	while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){
		printf("got page %d, %s\n",pagenum,buf);
		if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
			PF_PrintError("unfix");
			exit(1);
		}
	}
	if (error != PFE_EOF){
		PF_PrintError("not eof\n");
		exit(1);
	}
	printf("eof reached\n");

}
struct QueryParam{
	char gender;
	char maritalStatus;
	int salary;
};
struct QueryParam extractParamFromInput(char* input){
	char localInput[100];
	struct QueryParam queryParam;
	int index = 0;
	char* pch = NULL;
	//printf("data : %s\n",input);
	strcpy(localInput,input);
	pch = strtok(localInput,",");
	while(index<5){

		//printf("data : %s\n",pch);
		index++;
		pch = strtok(NULL,",");
	}
	queryParam.gender = *pch;
	pch = strtok(NULL,",");
	queryParam.salary = atoi(pch);
	pch = strtok(NULL,",");
	queryParam.maritalStatus = *pch;

	return queryParam;
}
int areEqual(struct QueryParam queryParamToSearch,struct QueryParam queryParamFromData)
{
	if(queryParamToSearch.gender!=queryParamFromData.gender)
		return 0;
	if(queryParamToSearch.salary!=queryParamFromData.salary)
		return 0;
	if(queryParamToSearch.maritalStatus!=queryParamFromData.maritalStatus)
		return 0;
	return 1;
}
int getRandomNum()

{
    int randomnumber;
    srand(time(NULL));
    randomnumber = rand() % 10;
    printf("random number : %d\n", randomnumber);
    return randomnumber;
}

void accessDataUsingindex(char* fname1,char*fname2,int* hashArray){
	int *buf,*dataBuf;
	FILE* fp,*fpNumQueries,*fpNumBlockAcess,*fpExeTime;
	struct QueryParam queryParamToSearch,queryParamFromData;
	int dataToBeStored,pageNumber,pagenum,numBlockAcess;
	int fd1,fd2,error;
	char input[100],dataInput[100],inputReadFromindex[100],dataRead[100];
	char* ptrToData = NULL,*saved;
	int countOfQury = 2,randomNumber,indexToFile = 0,queryProcessed = 0;
	clock_t t;
	double time_taken;

	if ((fd1=PF_OpenFile(fname1))<0){
		PF_PrintError("open file1");
		exit(1);
	}

	if ((fd2=PF_OpenFile(fname2))<0){
		PF_PrintError("open file2");
		exit(1);
	}

	fpNumBlockAcess = fopen("numBlockAccess","w");
	fpNumQueries = fopen("numQuries","w");
	fpExeTime = fopen("exeTime","w");
	fclose(fpNumBlockAcess);
	fclose(fpNumQueries);
	fclose(fpExeTime);

	countOfQury =2;
	while(countOfQury<=10000){

		t = clock();
		fp = fopen("query","r");
		fpNumBlockAcess = fopen("numBlockAccess","a");
		fpNumQueries = fopen("numQuries","a");
		randomNumber = getRandomNum();

		//fseek (fp, randomNumber*100, SEEK_SET);
		numBlockAcess = 0;
		queryProcessed = 0;
		indexToFile=0;
		while(1){

			if(!fgets(input,100,fp)){

				if(queryProcessed<countOfQury){
					fclose(fp);
					fp = fopen("query","r");
					memset(&input,0,sizeof(input));
					memset(&dataInput,0,sizeof(dataInput));
					continue;
				}
				else break;
			}
			else{
				if(indexToFile<randomNumber){
					indexToFile++;
					continue;
				}
			}
			if(queryProcessed>=countOfQury)
				break;
			queryProcessed++;
			printf("%s\n",input);
			strcpy(dataInput,input);
			queryParamToSearch = extractParamFromInput(dataInput);
			dataToBeStored=extractDataFromInputString(input);
			//printf("%s",dataInput);
			if(dataToBeStored>=0){
				pagenum = hashArray[dataToBeStored]-1;
				if((error=PF_GetNextPage(fd1,&pagenum,&buf))== PFE_OK){
					numBlockAcess++;
					strcpy(inputReadFromindex,buf);
					ptrToData = strtok_r(inputReadFromindex,",",&saved);
					ptrToData = strtok_r(NULL,",",&saved);
					while(ptrToData!=NULL){
						pageNumber = atoi(ptrToData)-1;
						if((error=PF_GetNextPage(fd2,&pageNumber,&dataBuf))== PFE_OK){
							strcpy(dataRead,dataBuf);
							numBlockAcess++;
							//printf("data : %s\n",dataRead);
							queryParamFromData = extractParamFromInput(dataRead);
							if(areEqual(queryParamToSearch,queryParamFromData)){
								//printf("data found at page number : %d\n",pageNumber);
							}
							if ((error=PF_UnfixPage(fd2,pageNumber,FALSE))!= PFE_OK){
								PF_PrintError("unfix");
								exit(1);
							}
						}

						ptrToData = strtok_r(NULL,",",&saved);
						//printf("page number : %s\n",ptrToData);
					}
					if ((error=PF_UnfixPage(fd1,pagenum,FALSE))!= PFE_OK){
						PF_PrintError("unfix");
						exit(1);
					}
				}
			}
			//queryParamToSearch = extractParamFromInput(dataInput);
			//printf("input data : %c : %d : %c\n",queryParamToSearch.gender,queryParamToSearch.salary,queryParamToSearch.maritalStatus);
			memset(&input,0,sizeof(input));
			memset(&dataInput,0,sizeof(dataInput));
		}

		fprintf(fpNumBlockAcess, "%d\n",numBlockAcess);
		fprintf(fpNumQueries, "%d\n",countOfQury);
		fclose(fpNumQueries);
		fclose(fpNumBlockAcess);
		fclose(fp);
		countOfQury = countOfQury+2;
		t = clock() - t;
		time_taken = ((double)t)/CLOCKS_PER_SEC;
		fpExeTime = fopen("exeTime","a");
		fprintf(fpExeTime, "%f\n",time_taken);
		fclose(fpExeTime);
	}
	if ((error=PF_CloseFile(fd1))!= PFE_OK){
		PF_PrintError("close file1\n");
		exit(1);
	}

	if ((error=PF_CloseFile(fd2))!= PFE_OK){
		PF_PrintError("close file2\n");
		exit(1);
	}


}
void accessDataWithoutindex(char* fname2){
	int *buf;
	FILE* fp,*fpNumBlockAcess,*fpExeTime;
	struct QueryParam queryParamToSearch,queryParamFromData;
	int pagenum,numBlockAcess;
	int fd2,error;
	char input[100],dataRead[100],dataInput[100];
	int countOfQury = 2,randomNumber,indexToFile = 0,queryProcessed = 0;
	clock_t t;
	double time_taken;

	if ((fd2=PF_OpenFile(fname2))<0){
		PF_PrintError("open file2");
		exit(1);
	}
	fpNumBlockAcess = fopen("numBlockAccessWithoutIndex","w");
	fpExeTime = fopen("exeTimeWithoutIndex","w");
	fclose(fpNumBlockAcess);
	fclose(fpExeTime);
	//fp = fopen("query","r");
	countOfQury =2;
	while(countOfQury<=10000){
		t = clock();
		fp = fopen("query","r");
		fpNumBlockAcess = fopen("numBlockAccessWithoutIndex","a");
		//fpNumQueries = fopen("numQuries","a");
		randomNumber = getRandomNum();

		//fseek (fp, randomNumber*100, SEEK_SET);
		numBlockAcess = 0;
		queryProcessed = 0;
		indexToFile = 0;
		while(1){
			if(!fgets(input,100,fp)){

				if(queryProcessed<countOfQury){
					fclose(fp);
					fp = fopen("query","r");
					memset(&input,0,sizeof(input));
					memset(&dataInput,0,sizeof(dataInput));
					continue;
				}
				else break;
			}
			else{
				if(indexToFile<randomNumber){
					indexToFile++;
					continue;
				}
			}
			if(queryProcessed>=countOfQury)
				break;
			queryProcessed++;
			//printf("%s",input);
			strcpy(dataInput,input);
			queryParamToSearch = extractParamFromInput(dataInput);

			pagenum = -1;
			while ((error=PF_GetNextPage(fd2,&pagenum,&buf))== PFE_OK){
				//printf("got page %d, %s\n",pagenum,buf);
				numBlockAcess++;
				strcpy(dataRead,buf);
				//printf("data : %s\n",dataRead);
				queryParamFromData = extractParamFromInput(dataRead);
				if(areEqual(queryParamToSearch,queryParamFromData)){
					printf("data found at page number without index : %d\n",pagenum);
				}
				if ((error=PF_UnfixPage(fd2,pagenum,FALSE))!= PFE_OK){
					PF_PrintError("unfix");
					exit(1);
				}
			}
			if (error != PFE_EOF){
				PF_PrintError("not eof\n");
				exit(1);
			}
		}
		fprintf(fpNumBlockAcess, "%d\n",numBlockAcess);
		///fprintf(fpNumQueries, "%d\n",countOfQury);
		//fclose(fpNumQueries);
		fclose(fpNumBlockAcess);
		fclose(fp);
		countOfQury = countOfQury+2;
		t = clock() - t;
		time_taken = ((double)t)/CLOCKS_PER_SEC;
		fpExeTime = fopen("exeTimeWithoutIndex","a");
		fprintf(fpExeTime, "%f\n",time_taken);
		fclose(fpExeTime);
	}
	if ((error=PF_CloseFile(fd2))!= PFE_OK){
		PF_PrintError("close file2\n");
		exit(1);
	}
}
int countBitMaps(int fd,int queryBitMap){
	int error;
	int *buf;
	int pagenum;
	int storedBitMap;
	int counter=0;
	int numberOfPageRead=0;
	char* ptrToData = NULL,*saved;
	char inputReadFromindex[4096];
	int noOfSeek=0;

//		printf("reading file\n");
		pagenum = -1;
		while ((error=PF_GetNextPage(fd,&pagenum,&buf))== PFE_OK){

			strcpy(inputReadFromindex,buf);
			ptrToData = strtok_r(inputReadFromindex,",",&saved);noOfSeek++;
			while(ptrToData!=NULL){
				storedBitMap = atoi(ptrToData);
				if(storedBitMap==queryBitMap){
					counter++;
				}
				ptrToData = strtok_r(NULL,",",&saved);noOfSeek++;
			}
//			printf("got page %d, %s\n",pagenum,buf);
			if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
				PF_PrintError("unfix");
				exit(1);
			}
			numberOfPageRead++;
		}
		if (error != PFE_EOF){
			PF_PrintError("not eof\n");
			exit(1);
		}
		printf("Number Of Page Read :%d And Number Of Seek :%d\n",numberOfPageRead,noOfSeek);

return counter;
}
int getNumberOfBitMapsPresentForGivenValue(char *bitMapFile,int bitMapValue)

{
int error;
int *buf;
int pagenum;
int fd;

//	printf("opening %s\n",bitMapFile);
	if ((fd=PF_OpenFile(bitMapFile))<0){
		PF_PrintError("open file");
		exit(1);
	}
	return countBitMaps(fd,bitMapValue);
	if ((error=PF_CloseFile(fd))!= PFE_OK){
		PF_PrintError("close file");
		exit(1);
	}
	return 0;
}
void getSelectOptions(int *returnNo,int selectOption[]){
	int noOfSelectOption;
		bool validityOfUserInput=false;

	printf("Enter Number Of SELECT options you Need : ");
	scanf("%d",&noOfSelectOption);
	if(noOfSelectOption>0 && noOfSelectOption<=MAX_SELECT_OPTION){
		validityOfUserInput=true;
		noOfSelectOption=noOfSelectOption-1;//index start from zero
	}
	while(!validityOfUserInput){
		printf("Number Of SELECT Option Should Be Between 1 And %d.Please Retry : ",MAX_SELECT_OPTION);
		scanf("%d",&noOfSelectOption);
		if(noOfSelectOption>0 && noOfSelectOption<=MAX_SELECT_OPTION){
			validityOfUserInput=true;
			noOfSelectOption=noOfSelectOption-1;//index start from zero
		}
	}
	printf("Option For SELECT Are:\n");
	printf("1.ATM Id\n2.Customer Id\n3.Gender\n4.Salary\n");
	for(int i=0;i<=noOfSelectOption;i++){
		validityOfUserInput=false;
		printf("Enter Value For Option%d : ",i+1);
		scanf("%d",&selectOption[i]);
		if(selectOption[i]>0 && selectOption[i]<=MAX_SELECT_OPTION){
			validityOfUserInput=true;
		}
		else{
			while(!validityOfUserInput){
				printf("SELECT Option Is Incorrect Please Retry : ");
				scanf("%d",&selectOption[i]);
				if(selectOption[i]>0 && selectOption[i]<=MAX_SELECT_OPTION){
					validityOfUserInput=true;
				}
			}
		}
	}
	*returnNo=noOfSelectOption;
//
//	for(int i=0;i<noOfSelectOption;i++){
//		printf("Option%d is %d\n",i+1,selectOption[i]);
//	}
}
bool geValueOnBasisOfComparisionType(int type,int comparisionValue[]){
	bool result=false;
	bool validityOfUserInput=false;
	if(comparisionValue[type]!=-1){
		printf("Two Comparision Of Same Type Is Not Allowed\n ");
		return false;
	}
	switch(type){

	case 0:
		printf("Enter Gender For Comparison (1 For Male  Or 2 For Female) : ");
		scanf("%d",&comparisionValue[type]);
		if(IS_VALID_GENDER(comparisionValue[type])){
			validityOfUserInput=true;
		}
		else{
			while(!validityOfUserInput){
				printf("Invalid Gender Please Retry : ");
				scanf("%d",&comparisionValue[type]);
				if(IS_VALID_GENDER(comparisionValue[type])){
					validityOfUserInput=true;
				}
			}
		}
		result=true;
		break;
	case 1:
		printf("Enter Amount For Comparison  : ");
		scanf("%d",&comparisionValue[type]);
		if(comparisionValue[type]>0){
			validityOfUserInput=true;
		}
		else{
			while(!validityOfUserInput){
				printf("Invalid Amount Please Retry : ");
				scanf("%d",&comparisionValue[type]);
				if(comparisionValue[type]>0){
					validityOfUserInput=true;
				}
			}
		}
		result=true;
		break;
	case 2:
		printf("Enter Marital Status For Comparsion (1 For Single  Or 2 For Married) : ");
		scanf("%d",&comparisionValue[type]);
		if(IS_VALID_MARITAL_STATUS(comparisionValue[type])){
			validityOfUserInput=true;
		}
		else{
			while(!validityOfUserInput){
				printf("Invalid Gender Please Retry : ");
				scanf("%d",&comparisionValue[type]);
				if(IS_VALID_MARITAL_STATUS(comparisionValue[type])){
					validityOfUserInput=true;
				}
			}
		}
		result=true;
		break;

	default:
		return false;
	}
	return result;
}
char * getOptionFromInt(int optionNumber){
	switch(optionNumber){
		case 1:
			return "Count(*)";

		case 2:
			return "Customer Id";

		case 3:
			return "Gender";
		case 4:
			return "Salary";

	}
	return "";
}
char * getComparisonFromInt(int comType,int comValue){
	switch(comType){
	case 0://for Gender
		return comValue==1?"GENDER='Male'":"GENDER='Female'";

	case 1://For Salary
		return SALARY_TYPE(comValue);

	case 2://For status
		return comValue==1?"MARITAL_STATUS='Single'":"MARITAL_STATUS='Married'";


	}
	return "";
}
void getComparisionTypeAndValue(int *returnComNo,int comparisionType[],int comparisionValue[]){
	bool validityOfUserInput=false;
	int noOfComparison=3;//Defaulting  for simplicity
	validityOfUserInput=false;
//	printf("Enter Number Of Comparison  You Need (Max Allowed %d) : ",MAX_COMPARISION_OPTION);
//	scanf("%d",&noOfComparison);
//	if(noOfComparison>0 && noOfComparison<=MAX_COMPARISION_OPTION){
//			validityOfUserInput=true;
//		}
//		while(!validityOfUserInput){
//			printf("Number Of Comparison Should Be Between 1 And %d.Please Retry : ",MAX_COMPARISION_OPTION);
//			scanf("%d",&noOfComparison);
//			if(noOfComparison>0 && noOfComparison<=MAX_COMPARISION_OPTION){
//				validityOfUserInput=true;
//			}
//		}
//	printf("Valid  Comparison Types Allowed :\n 1. For Gender \n 2. For Amount \n 3. For Marital Status \n ");
	for(int comNo=0;comNo<noOfComparison;comNo++){
//		printf("Enter Type For Comparison %d : ",comNo+1);
//		scanf("%d",&comparisionType[comNo]);
		comparisionType[comNo]=comNo;
		validityOfUserInput=false;
		while(!validityOfUserInput){
			if(geValueOnBasisOfComparisionType(comparisionType[comNo],comparisionValue)){
				validityOfUserInput=true;
			}else{
				printf("Enter Type For Comparison %d : ",comNo+1);
				scanf("%d",&comparisionType[comNo]);
			}

		}
	}
	*returnComNo=noOfComparison;
//	for(int i=0;i<noOfComparison;i++){
//		printf("ComparisonType%d is %d \n",i+1,comparisionType[i]);
//		printf("ComparisonValue%d is %d \n",i+1,comparisionValue[i]);
//	}

}
void generateQuery(int noOfSelectOption,int noOfComparison,int selectOption[],int comparisionType[],int comparisionValue[]){
	if(noOfSelectOption>-1){
		printf("SELECT ");
		for(int optionNo=0;optionNo<=noOfSelectOption;optionNo++){
			if(optionNo==0){
				printf("%s",getOptionFromInt(selectOption[optionNo]));
			}else{
				printf(",%s",getOptionFromInt(selectOption[optionNo]));
			}
		}
	}
	printf(" FROM %s\n",RELATION_NAME);
	printf(" WHERE ");
	if(noOfComparison>0){
		for(int compNo=0;compNo<noOfComparison;compNo++){
			if(compNo==0){
				printf("%s",getComparisonFromInt(comparisionType[compNo],comparisionValue[compNo]));
			}else{
				printf(" AND %s",getComparisonFromInt(comparisionType[compNo],comparisionValue[compNo]));
			}
		}
	}
	printf("\n");

}
int generateBitMapForQuery(int comparisionValue[]){

	int bitMap=0;
	if(comparisionValue[0]==1){
		bitMap+=64;//for male
	}else if(comparisionValue[0]==2){
		bitMap+=32;//for female
	}

	bitMap+=GET_BIT_MAP_FOR_SALARY(comparisionValue[1]);//comparisionValue[1] is amount
	if(comparisionValue[2]==1){
		bitMap+=2;//for single
	}else if(comparisionValue[2]==2){
		bitMap+=1;//for married
	}
	return bitMap;
}
void queryDatabase(char * bitMapFile){
	int noOfSelectOption=-1;
		int noOfComparison=-1;
		int selectOption[MAX_SELECT_OPTION],comparisionType[MAX_COMPARISION_OPTION],comparisionValue[MAX_COMPARISION_OPTION];
		for(int i=0;i<MAX_COMPARISION_OPTION;i++){
			comparisionValue[i]=-1;
		}
		printf("Query Format:\n");
		printf("SELECT count(*) From %s WHERE GENDER=? AND SALARY=? AND MARITAL_STATUS=?\n",RELATION_NAME);

	//	getSelectOptions(&noOfSelectOption,selectOption);
		noOfSelectOption=0;
		selectOption[noOfSelectOption]=1;
		getComparisionTypeAndValue(&noOfComparison,comparisionType,comparisionValue);
		generateQuery(noOfSelectOption,noOfComparison,selectOption,comparisionType,comparisionValue);
		int bitmap=generateBitMapForQuery(comparisionValue);
		printf("Output For the Above Query Is : %d\n\n",getNumberOfBitMapsPresentForGivenValue(bitMapFile,bitmap));
}
int main(){
	int error;
	int hashArray[10000];
	/* create a few files */
	remove(FILE1);
	remove(FILE2);
	int indexArray[4096];
	memset(&indexArray, -1, sizeof indexArray);
	memset(&hashArray,-1,sizeof(hashArray));
	if ((error=PF_CreateFile(FILE1))!= PFE_OK){
		PF_PrintError("file1");
		exit(1);
	}
	printf("file1 created\n");

	if ((error=PF_CreateFile(FILE2))!= PFE_OK){
		PF_PrintError("file2");
	exit(1);
	}
	printf("file2 created\n");
	sequentialWrite(FILE1,FILE2,hashArray);

//	while(1){
//		printf("Enter Query bitmap :");
//		scanf("%d",&option);
//		printf("For the Given Query bit map number of record present are : %d\n",getNumberOfBitMapsPresentForGivenValue(FILE1,option));
//
//
//	}

	readfileData(FILE1);
//	readfileData(FILE2);
//	accessDataUsingindex(FILE1,FILE2,hashArray);
//	accessDataWithoutindex(FILE2);
	printf("Total Number Of Data Present :%d\n",totalNoOfData);
	while(1){
		queryDatabase(FILE1);
	}


return 0;
}


