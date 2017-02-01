/*
 * -*-C-*-
 * slev.pc 
 * corresponds to A.5 in appendix A
 */
 
#include <string.h>
#include <stdio.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;
extern struct runtime_log query_log[];

/*
 * the stock level transaction
 */
int slev( int t_num,
	  int w_id_arg,		/* warehouse id */
	  int d_id_arg,		/* district id */
	  int level_arg		/* stock level */
)
{
#ifdef OUT_PERIOD
 outputLog("s0|");
#endif
	int            w_id = w_id_arg;
	int            d_id = d_id_arg;
	int            level = level_arg;
	int            d_next_o_id;
	int            i_count;
	int            ol_i_id;

	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[4];
        MYSQL_BIND    column[1];
	MYSQL_STMT*   mysql_stmt2;
        MYSQL_BIND    param2[3];
        MYSQL_BIND    column2[1];

/* goda [ */
	int	*sqlresult;
	int	resultlimit = 100;
	int	resultnum;
	int	i;
	int	tempnum;
	char	tempquery[500];
	MYSQL_RES *resp = NULL;
	MYSQL_ROW row;

	struct timespec tbuf;
	double runtime;

	sqlresult = (int *)malloc(sizeof(int) * resultlimit);
	if(sqlresult == NULL){
		outputLog("malloc error slev sqlresult");
		free(sqlresult);
		exit(-1);
	}
/* ] goda */ 

	/*EXEC SQL WHENEVER NOT FOUND GOTO sqlerr;*/
	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

	/* find the next order id */
#ifdef DEBUG
	printf("select 1\n");
#endif
	/*EXEC_SQL SELECT d_next_o_id
	                INTO :d_next_o_id
	                FROM district
	                WHERE d_id = :d_id
			AND d_w_id = :w_id;*/
/* goda commentout
	mysql_stmt = stmt[t_num][32];

	memset(param, 0, sizeof(MYSQL_BIND) * 2); /* initialize *
	param[0].buffer_type = MYSQL_TYPE_LONG;
	param[0].buffer = &d_id;
	param[1].buffer_type = MYSQL_TYPE_LONG;
	param[1].buffer = &w_id;
	if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
	if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

	if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
	memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize *
	column[0].buffer_type = MYSQL_TYPE_LONG;
	column[0].buffer = &d_next_o_id;
	if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
	switch( mysql_stmt_fetch(mysql_stmt) ) {
	    case 0: //SUCCESS
		break;
	    case 1: //ERROR
	    case MYSQL_NO_DATA: //NO MORE DATA
	    default:
		mysql_stmt_free_result(mysql_stmt);
		goto sqlerr;
	}
	mysql_stmt_free_result(mysql_stmt);
*/
/* goda [ */
#ifdef OUT_PERIOD
 outputLog("s1|");
#endif
	sprintf(tempquery,"SELECT d_next_o_id FROM district WHERE d_id = %d AND d_w_id = %d",d_id,w_id);
	GetStartTime(&tbuf);
	if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
	runtime = GetRunTime(&tbuf);
	query_log[32].runCount++;
	query_log[32].runTime += runtime;

	resp = mysql_use_result(ctx[t_num]);
	if((row = mysql_fetch_row(resp)) != NULL){
		d_next_o_id = atoi(row[0]);
	}
	else{
		//NO RESULT
		mysql_free_result(resp);
		goto sqlerr;
	}
	mysql_free_result(resp);
/* ] goda */ 

	/* find the most recent 20 orders for this district */
	/*EXEC_SQL DECLARE ord_line CURSOR FOR
	                SELECT DISTINCT ol_i_id
	                FROM order_line
	                WHERE ol_w_id = :w_id
			AND ol_d_id = :d_id
			AND ol_o_id < :d_next_o_id
			AND ol_o_id >= (:d_next_o_id - 20);

	EXEC_SQL OPEN ord_line;

	EXEC SQL WHENEVER NOT FOUND GOTO done;*/

/* goda commentout
	mysql_stmt = stmt[t_num][33];

	memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize *
	param[0].buffer_type = MYSQL_TYPE_LONG;
	param[0].buffer = &w_id;
	param[1].buffer_type = MYSQL_TYPE_LONG;
	param[1].buffer = &d_id;
	param[2].buffer_type = MYSQL_TYPE_LONG;
	param[2].buffer = &d_next_o_id;
	param[3].buffer_type = MYSQL_TYPE_LONG;
	param[3].buffer = &d_next_o_id;
	if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
	if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

	if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
	memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize *
	column[0].buffer_type = MYSQL_TYPE_LONG;
	column[0].buffer = &ol_i_id;
	if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
*/
/* goda [ */
#ifdef OUT_PERIOD
 outputLog("s2|");
#endif
	tempnum = d_next_o_id - 20;
	sprintf(tempquery,"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id < %d AND ol_o_id >= %d",w_id,d_id,d_next_o_id,tempnum);
//printf("\ns2 query:%s\n",tempquery);
	GetStartTime(&tbuf);
	if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
	runtime = GetRunTime(&tbuf);
	query_log[33].runCount++;
	query_log[33].runTime += runtime;

	resp = mysql_use_result(ctx[t_num]);

	resultnum = 0;
	while((row = mysql_fetch_row(resp)) != NULL){
		if(resultnum == resultlimit){
			//expand sqlresult
			resultlimit += 100;
			sqlresult = (int *)realloc(sqlresult, sizeof(int) * resultlimit);
			if(sqlresult == NULL){
				outputLog("realloc error slev");
				free(sqlresult);
				exit(-1);
			}
		}
		sqlresult[resultnum] = atoi(row[0]);
		resultnum++;
	}
	mysql_free_result(resp);
	i=0;
/* ] goda */ 

	for (;;) {
#ifdef DEBUG
		printf("fetch 1\n");
#endif
		/*EXEC_SQL FETCH ord_line INTO :ol_i_id;*/
/* goda commentout
		switch( mysql_stmt_fetch(mysql_stmt) ) {
                    case 0: //SUCCESS
                        break;
                    case MYSQL_NO_DATA: //NO MORE DATA
                        mysql_stmt_free_result(mysql_stmt);
                        goto done;
                    case 1: //ERROR
                    default:
                        mysql_stmt_free_result(mysql_stmt);
                        goto sqlerr;
                }
*/

		if(i < resultnum){
			ol_i_id = sqlresult[resultnum];
			i++;
		}
		else{
			goto done;
		}



/*
		if((row = mysql_fetch_row(resp)) != NULL){
printf("sf|");
fflush(stdout);
			ol_i_id = atoi(row[0]);
		}
		else{
printf("sff|");
fflush(stdout);
			//NO RESULT
			mysql_free_result(resp);
			goto done;
		}
*/

#ifdef DEBUG
		printf("select 2\n");
#endif

		/*EXEC_SQL SELECT count(*) INTO :i_count
			FROM stock
			WHERE s_w_id = :w_id
		        AND s_i_id = :ol_i_id
			AND s_quantity < :level;*/
/* goda commentout
		mysql_stmt2 = stmt[t_num][34];

		memset(param2, 0, sizeof(MYSQL_BIND) * 3); /* initialize *
		param2[0].buffer_type = MYSQL_TYPE_LONG;
		param2[0].buffer = &w_id;
		param2[1].buffer_type = MYSQL_TYPE_LONG;
		param2[1].buffer = &ol_i_id;
		param2[2].buffer_type = MYSQL_TYPE_LONG;
		param2[2].buffer = &level;
		if( mysql_stmt_bind_param(mysql_stmt2, param2) ) goto sqlerr2;
		if( mysql_stmt_execute(mysql_stmt2) ) goto sqlerr2;

		if( mysql_stmt_store_result(mysql_stmt2) ) goto sqlerr2;
		memset(column2, 0, sizeof(MYSQL_BIND) * 1); /* initialize *
		column2[0].buffer_type = MYSQL_TYPE_LONG;
		column2[0].buffer = &i_count;
		if( mysql_stmt_bind_result(mysql_stmt2, column2) ) goto sqlerr2;
		switch( mysql_stmt_fetch(mysql_stmt2) ) {
		    case 0: //SUCCESS
			break;
		    case 1: //ERROR
		    case MYSQL_NO_DATA: //NO MORE DATA
		    default:
			mysql_stmt_free_result(mysql_stmt2);
			goto sqlerr2;
		}
		mysql_stmt_free_result(mysql_stmt2);
*/
/* goda [ */
#ifdef OUT_PERIOD
 outputLog("s3|");
#endif
		sprintf(tempquery,"SELECT count(*) FROM stock WHERE s_w_id = %d AND s_i_id = %d AND s_quantity < %d",w_id,ol_i_id,level);
//printf("\ns3 query:%s\n",tempquery);
fflush(stdout);

		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr2;
		runtime = GetRunTime(&tbuf);
		query_log[34].runCount++;
		query_log[34].runTime += runtime;

		resp = mysql_use_result(ctx[t_num]);
		if((row = mysql_fetch_row(resp)) != NULL){
			i_count = atoi(row[0]);
		}
		else{
			//NO RESULT
			mysql_free_result(resp);
			goto sqlerr2;
		}
		mysql_free_result(resp);
/* ] goda */ 

	}

done:
	/*EXEC_SQL CLOSE ord_line;*/
	/*EXEC_SQL COMMIT WORK;*/
#ifdef OUT_PERIOD
 outputLog("sd|");
#endif
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;
	free(sqlresult);

	return (1);

sqlerr:
        fprintf(stderr,"slev1\n");
	outputQuery(tempquery);
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	free(sqlresult);
	return (0);

sqlerr2:
        fprintf(stderr,"slev2\n");
	outputQuery(tempquery);
	error(ctx[t_num],mysql_stmt2);
	//printf("\nresp:%x,resp2:%x\n",resp,resp2);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	//mysql_stmt_free_result(mysql_stmt);
	mysql_rollback(ctx[t_num]);
	free(sqlresult);
	return (0);
}
