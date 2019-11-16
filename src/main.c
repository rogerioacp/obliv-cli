/*
 * src/test/examples/testlibpq.c
 *
 *
 * testlibpq.c
 *
 *		Test the C version of libpq, the PostgreSQL frontend library.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libpq-fe.h"

#define FILE_BUFFER_SIZE 1000
#define INPUT_SIZE 10

typedef enum {
    DYNAMIC = 0,
    OST =1
}Mode;



static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}


PGconn* establish_connection(char* conninfo){

    PGconn      *conn;

    printf("Establish connection with connection string %s.\n", conninfo);

	conn = PQconnectdb(conninfo);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s\n",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}


    return conn;
}

void init_oblivpg(PGconn *conn, Mode mode){

    PGresult    *res;
    
    printf("Open enclave.\n");
    res = PQexec(conn, "select open_enclave()");
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Failed to open enclave: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    PQclear(res);
    printf("Initiate SOE in mode %d.\n", mode);

    if(mode == DYNAMIC){
        res = PQexec(conn, "select init_soe(0, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER))");

    }else{
        res = PQexec(conn, "select init_soe(1, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER))");
    }
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Failed to initialize SOE: %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    
    PQclear(res);

}

void load_blocks(PGconn *conn){
    PGresult    *res;
    
    printf("Load Blocks.\n");
    res = PQexec(conn, "select load_blocks(CAST (get_original_index_oid() as INTEGER),CAST(get_original_heap_oid() as INTEGER))");

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Failed to load blocks:  %s\n", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    
    PQclear(res);
}

void insert_tuples(PGconn *conn, char* file_path){

    FILE        *fp;
    PGresult    *res;
    char        row[FILE_BUFFER_SIZE];

    printf("Inserting tuples from %s on table.\n", file_path);
    //fp = fopen("src/inserts_obl_full.sql", "r");
    fp = fopen(file_path, "r");

    if(fp == NULL){
        perror("Unable to open file!\n");
        exit(1);
    }


    while(fgets(row, FILE_BUFFER_SIZE, fp)!= NULL){

        res = PQexec(conn, row);
        
       
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            fprintf(stderr, "Failed to insert row: %s\n",  PQerrorMessage(conn));
        }
        PQclear(res);
    }

    fclose(fp);
}

int
main(int argc, char **argv)
{

    // PG vars
	PGconn	    *conn;
	PGresult    *res;

    //oblivpg state
    Mode        emode;
    char*       file_path;

    //output
	int			nFields;
	int			i,j;
    int         first = 0;

    //input
    char        uinput[INPUT_SIZE];

    
    if(argc < 1){
        fprintf(stderr, "Expecting input execution mode and insertion file path\n");
    }else{
        emode = atoi(argv[1]);
        file_path = argv[2];
    }

  	/*
	 * If the user supplies a parameter on the command line, use it as the
	 * conninfo string; otherwise default to setting dbname=postgres and using
	 * environment variables or defaults for all other connection parameters.
	 */
	if (argc > 3)
		conn = establish_connection(argv[3]);
	else
	    conn = establish_connection("dbname=teste host=127.0.0.1 port=5432");

    

    if(emode == DYNAMIC){
        printf("Dynamic mode insertion.\n");
        init_oblivpg(conn, emode);
        insert_tuples(conn, file_path);
    }else{
        printf("OST insertion mode.\n");
        insert_tuples(conn, file_path);
        init_oblivpg(conn, emode);
        load_blocks(conn);

    }


    printf("Start transaction.\n");
    res = PQexec(conn, "BEGIN");

    if(PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    
    PQclear(res);
    
    printf("Declare cursor.\n");

    res = PQexec(conn, "DECLARE tcursor CURSOR FOR select datereceived, state, zip from ftw_cfpb");

    if(PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "declare cursor failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    PQclear(res);
    
    printf("Fetch results.\n\n");

    while((res = PQexec(conn, "FETCH NEXT in tcursor"))!= NULL){
        
        if(PQresultStatus(res) != PGRES_TUPLES_OK){
            fprintf(stderr, "declare cursor failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }           

        if(!first){
   	        /* first, print out the attribute names */
	        nFields = PQnfields(res);
	        for (i = 0; i < nFields; i++)
		        printf("%-30s", PQfname(res, i));
	            printf("\n\n");
                first=1;
        }
	    
        
        /* next, print out the rows */
	    for (i = 0; i < PQntuples(res); i++)
	    {
		    for (j = 0; j < nFields; j++)
			    printf("%-30s", PQgetvalue(res, i, j));
		    printf("\n");
	    }

	    PQclear(res);
        fgets(uinput, INPUT_SIZE, stdin);

        if(strcmp(uinput, "q\n")==0){
            break;
        }
    }
    
    /* close the portal ... we don't bothe to check for errors ...*/
    res = PQexec(conn, "CLOSE tcursor");
    PQclear(res);

    /* end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);

    res = PQexec(conn, "select close_enclave()");
    PQclear(res);


    /* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}
