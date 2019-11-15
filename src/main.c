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

static void
exit_nicely(PGconn *conn)
{
	PQfinish(conn);
	exit(1);
}

int
main(int argc, char **argv)
{
	const char *conninfo;
	PGconn	   *conn;
	PGresult   *res;
	int			nFields;
	int			i,
				j, sRes;
    int         first = 0;
    FILE        *fp;
    char        row[1000];
    char        uinput[30];
    int         line = 1;

	/*
	 * If the user supplies a parameter on the command line, use it as the
	 * conninfo string; otherwise default to setting dbname=postgres and using
	 * environment variables or defaults for all other connection parameters.
	 */
	if (argc > 1)
		conninfo = argv[1];
	else
		conninfo = "dbname=teste host=127.0.0.1 port=5432";

	/* Make a connection to the database */
	conn = PQconnectdb(conninfo);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Connection to database failed: %s",
				PQerrorMessage(conn));
		exit_nicely(conn);
	}


    res = PQexec(conn, "select open_enclave()");
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Open Enclave failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    PQclear(res);

    
    res = PQexec(conn, "select init_soe(0, CAST( get_ftw_oid() as INTEGER), 1, CAST (get_original_index_oid() as INTEGER))");
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "init_soe failed  %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    
    PQclear(res);


    
    fp = fopen("src/inserts_obl_full.sql", "r");

    if(fp == NULL){
        perror("Unable to open file!");
        exit(1);
    }


    while(fgets(row,sizeof(row), fp)!= NULL){
        //printf("%s\n", row);

        res = PQexec(conn, row);
        
       
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            printf("Insert result is %s\n", PQresStatus(PQresultStatus(res)));
            printf("insert row %s at line number %d  failed with error: %s\n",row, line, PQerrorMessage(conn));
            //PQclear(res);
           // exit_nicely(conn);
        }
        PQclear(res);
        line++;
    }

    fclose(fp);
    
   
    printf("Start transaction\n");
    res = PQexec(conn, "BEGIN");

    if(PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "BEGIN command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    
    PQclear(res);
    
    printf("Declare cursor\n");

    res = PQexec(conn, "DECLARE tcursor CURSOR FOR select datereceived, state, zip from ftw_cfpb");

    if(PQresultStatus(res) != PGRES_COMMAND_OK){
        fprintf(stderr, "declare cursor failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    PQclear(res);


    while((res = PQexec(conn, "FETCH NEXT in tcursor"))!= NULL){
        
        if(PQresultStatus(res) != PGRES_TUPLES_OK){
            fprintf(stderr, "declare cursor failed: %s", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }           

        //printf("Got new result\n");
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
        gets(uinput);
        if(strcmp(uinput, "q")==0){
            break;
        }
    }
    
    /* close the portal ... we don't bothe to check for errors ...*/
    res = PQexec(conn, "CLOSE tcursor");
    PQclear(res);

    /* end the transaction */
    res = PQexec(conn, "END");
    PQclear(res);


    /* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}
