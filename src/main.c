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
    char        row[500];
    char        uinput[30];


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

    /*res = PQexec(conn, "select set_queueoid(16410);");

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        fprintf(stderr, "Open Enclave failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }

    PQclear(res);*/
    
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
    
    fp = fopen("src/inserts_obl.sql", "r");

    if(fp == NULL){
        perror("Unable to open file!");
        exit(1);
    }


    while(fgets(row,sizeof(row), fp)!= NULL){
        //printf("%s\n", row);

        res = PQexec(conn, row);
        
        //printf("Insert result is %s\n", PQresStatus(PQresultStatus(res)));

        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            fprintf(stderr, "insert row failed: %s\n", PQerrorMessage(conn));
            PQclear(res);
            exit_nicely(conn);
        }
        PQclear(res);

    }

    fclose(fp);
    
   
     printf("Sent Query\n");

    //sRes = PQsendQuery(conn, "select datereceived, product, state  from ftw_cfpb where datereceived = '06/12/2019'");
    //sRes = PQsendQuery(conn, "select datereceived, product, state  from ftw_cfpb where datereceived = '0'");
    sRes = PQsendQuery(conn, "select datereceived, product, state  from ftw_cfpb;");

    if(sRes == 0){
        fprintf(stderr, "select query failed: %s", PQerrorMessage(conn));
		exit_nicely(conn);
    }

    printf("setting single Row Mode\n");
    sRes = PQsetSingleRowMode(conn);

    if(sRes == 0){
        fprintf(stderr, "Set single row mode failed  %s", PQerrorMessage(conn));
		exit_nicely(conn);
    }
   
    printf("Going to retrieve results\n");

    while((res = PQgetResult(conn))!= NULL){
        
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


    /* close the connection to the database and cleanup */
	PQfinish(conn);

	return 0;
}
