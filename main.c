/* testpf.c */
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include "pf.h"
#include "pftypes.h"
#include <string.h>
#include <time.h>

#define FILE1	"file1"
#define FILE2	"file2"

#define LOW_LIMIT(salary) salary<500?1:0
#define MID_LIMIT(salary) (salary>=500&&salary<1000)?1:0
#define HIGH_LIMIT(salary) salary>=1000?1:0



main2()
{
int error;
int i;
int pagenum,*buf;
int *buf1,*buf2;
int fd1,fd2;



	/* create a few files */
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

	/* write to file1 */
	writefile(FILE1);

	/* print it out */
	readfile(FILE1);

	/* write to file2 */
	writefile(FILE2);

	/* print it out */
	readfile(FILE2);


	/* open both files */
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open file1\n");
		exit(1);
	}
	printf("opened file1\n");

	if ((fd2=PF_OpenFile(FILE2))<0 ){
		PF_PrintError("open file2\n");
		exit(1);
	}
	printf("opened file2\n");

	/* get rid of records  1, 3, 5, etc from file 1,
	and 0,2,4,6 from file2 */
	for (i=0; i < PF_MAX_BUFS; i++){
		if (i & 1){
			if ((error=PF_DisposePage(fd1,i))!= PFE_OK){
				PF_PrintError("dispose\n");
				exit(1);
			}
			printf("disposed %d of file1\n",i);
		}
		else {
			if ((error=PF_DisposePage(fd2,i))!= PFE_OK){
				PF_PrintError("dispose\n");
				exit(1);
			}
			printf("disposed %d of file2\n",i);
		}
	}

	if ((error=PF_CloseFile(fd1))!= PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}
	printf("closed file1\n");

	if ((error=PF_CloseFile(fd2))!= PFE_OK){
		PF_PrintError("close fd2");
		exit(1);
	}
	printf("closed file2\n");
	/* print the files */
	readfile(FILE1);
	readfile(FILE2);


	/* destroy the two files */
	if ((error=PF_DestroyFile(FILE1))!= PFE_OK){
		PF_PrintError("destroy file1");
		exit(1);
	}
	if ((error=PF_DestroyFile(FILE2))!= PFE_OK){
		PF_PrintError("destroy file2");
		exit(1);
	}

	/* create them again */
	if ((fd1=PF_CreateFile(FILE1))< 0){
		PF_PrintError("create file1");
		exit(1);
	}
	printf("file1 created\n");

	if ((fd2=PF_CreateFile(FILE2))< 0){
		PF_PrintError("create file2");
		exit(1);
	}
	printf("file2 created\n");

	/* put stuff into the two files */
	writefile(FILE1);
	writefile(FILE2);

	/* Open the files, and see how the buffer manager
	handles more insertions, and deletions */
	/* open both files */
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open file1\n");
		exit(1);
	}
	printf("opened file1\n");

	if ((fd2=PF_OpenFile(FILE2))<0 ){
		PF_PrintError("open file2\n");
		exit(1);
	}
	printf("opened file2\n");

	for (i=PF_MAX_BUFS; i < PF_MAX_BUFS*2 ; i++){
		if ((error=PF_AllocPage(fd2,&pagenum,&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		*((int *)buf) = i;
		if ((error=PF_UnfixPage(fd2,pagenum,TRUE))!= PFE_OK){
			PF_PrintError("unfix file1");
			exit(1);
		}
		printf("alloc %d file1\n",i,pagenum);

		if ((error=PF_AllocPage(fd1,&pagenum,&buf))!= PFE_OK){
			PF_PrintError("first buffer\n");
			exit(1);
		}
		*((int *)buf) = i;
		if ((error=PF_UnfixPage(fd1,pagenum,TRUE))!= PFE_OK){
			PF_PrintError("dispose file1");
			exit(1);
		}
		printf("alloc %d file2\n",i,pagenum);
	}

	for (i= PF_MAX_BUFS; i < PF_MAX_BUFS*2; i++){
		if (i & 1){
			if ((error=PF_DisposePage(fd1,i))!= PFE_OK){
				PF_PrintError("dispose fd1");
				exit(1);
			}
			printf("dispose fd1 page %d\n",i);
		}
		else {
			if ((error=PF_DisposePage(fd2,i))!= PFE_OK){
				PF_PrintError("dispose fd2");
				exit(1);
			}
			printf("dispose fd2 page %d\n",i);
		}
	}

	printf("getting file2\n");
	for (i=PF_MAX_BUFS; i < PF_MAX_BUFS*2; i++){
		if (i & 1){
			if ((error=PF_GetThisPage(fd2,i,&buf))!=PFE_OK){
				PF_PrintError("get this on fd2");
				exit(1);
			}
			printf("%d %d\n",i,*buf);
			if ((error=PF_UnfixPage(fd2,i,FALSE))!= PFE_OK){
				PF_PrintError("get this on fd2");
					exit(1);
			}
		}
	}

	printf("getting file1\n");
	for (i=PF_MAX_BUFS; i < PF_MAX_BUFS*2; i++){
		if (!(i & 1)){
			if ((error=PF_GetThisPage(fd1,i,&buf))!=PFE_OK){
				PF_PrintError("get this on fd2");
				exit(1);
			}
			printf("%d %d\n",i,*buf);
			if ((error=PF_UnfixPage(fd1,i,FALSE))!= PFE_OK){
				PF_PrintError("get this on fd2");
					exit(1);
			}
		}
	}

	/* print the files */
	printfile(fd2);

	printfile(fd1);

	/*put some more stuff into file1 */
	printf("putting stuff into holes in fd1\n");
	for (i=0; i < (PF_MAX_BUFS/2 -1); i++){
		if (PF_AllocPage(fd1,&pagenum,&buf)!= PFE_OK){
			PF_PrintError("PF_AllocPage");
			exit(1);
		}
		*buf =pagenum;
		if (PF_UnfixPage(fd1,pagenum,TRUE)!= PFE_OK){
			PF_PrintError("PF_UnfixPage");
			exit(1);
		}
	}

	printf("printing fd1");
	printfile(fd1);

	PF_CloseFile(fd1);
	printf("closed file1\n");

	PF_CloseFile(fd2);
	printf("closed file2\n");

	/* open file1 twice */
	if ((fd1=PF_OpenFile(FILE1))<0){
		PF_PrintError("open file1");
		exit(1);
	}
	printf("opened file1\n");

	/* try to destroy it while it's still open*/
	error=PF_DestroyFile(FILE1);
	PF_PrintError("destroy file1, should not succeed");


	/* get rid of some invalid page */
	error=PF_DisposePage(fd1,100);
	PF_PrintError("dispose page 100, should fail");


	/* get a valid page, and try to dispose it without unfixing.*/
	if ((error=PF_GetThisPage(fd1,1,&buf))!=PFE_OK){
		PF_PrintError("get this on fd2");
		exit(1);
	}
	printf("got page%d\n",*buf);
	error=PF_DisposePage(fd1,1);
	PF_PrintError("dispose page1, should fail");

	/* Now unfix it */
	if ((error=PF_UnfixPage(fd1,1,FALSE))!= PFE_OK){
		PF_PrintError("get this on fd2");
			exit(1);
	}

	error=PF_UnfixPage(fd1,1,FALSE);
	PF_PrintError("unfix fd1 again, should fail");

	if ((fd2=PF_OpenFile(FILE1))<0 ){
		PF_PrintError("open file1 again");
		exit(1);
	}
	printf("opened file1 again\n");

	printfile(fd1);

	printfile(fd2);

	if (PF_CloseFile(fd1) != PFE_OK){
		PF_PrintError("close fd1");
		exit(1);
	}

	if (PF_CloseFile(fd2)!= PFE_OK){
		PF_PrintError("close fd2");
		exit(1);
	}

	/* print the buffer */
	printf("buffer:\n");
	PFbufPrint();

	/* print the hash table */
	printf("hash table:\n");
	PFhashPrint();
}
/**
 * write data in file having file descriptor as fd.
 * data is written sequentially so we allocate the page write data and unfix data page as we do not require it again
 *
 */
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
			sprintf(pageNumberStr,"%d",pageNumber);
			strcat(buf,",");
			strcat(buf,pageNumberStr);
			if ((error=PF_UnfixPage(fd,pagenum,FALSE))!= PFE_OK){
				PF_PrintError("unfix");
				exit(1);
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
	//*((int *)buf) = data;
	printf("data written to memory %s \n",data);
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
	dataTokensToRead = strtok(NULL,",");
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
void sequentialWrite(char *fname1,char * fname2,int* hashArray){
	int fd1,fd2,error,pageNumber;
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

	fp = fopen("testingData","r");
	while(fgets(input,100,fp)){
		printf("%s",input);
		strcpy(dataInput,input);
		dataToBeStored=extractDataFromInputString(input);
		printf("%s",dataInput);
		if(dataToBeStored>=0){
			pageNumber = sequentialWriteInternalData(fd2,dataInput);
			sequentialWriteIndex(fd1,dataToBeStored,hashArray,pageNumber);

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

	readfileData(FILE1);
	readfileData(FILE2);
	accessDataUsingindex(FILE1,FILE2,hashArray);
	accessDataWithoutindex(FILE2);

return 0;
}


