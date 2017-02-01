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

/*
 * the stock level transaction
 */
int slev( int t_num,
	  int w_id_arg,		/* warehouse id */
	  int d_id_arg,		/* district id */
	  int level_arg		/* stock level */
)
{
printf("s0|");
fflush(stdout);
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
	int	sqlresult[100];
	char	tempquery[500];
	char	tempquery2[500];
	MYSQL_RES *resp = NULL;
	MYSQL_RES *resp2 = NULL;
	MYSQL_ROW row;
	MYSQL_ROW row2;
	MYSQL *ctx2;
	ctx2 = mysql_init(NULL);
	*ctx2 = *ctx[t_num];
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
printf("s1|");
fflush(stdout);
	sprintf(tempquery,"SELECT d_next_o_id FROM district WHERE d_id = %d AND d_w_id = %d",d_id,w_id);
	if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
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
printf("s2|");
fflush(stdout);
	sprintf(tempquery,"SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = %d AND ol_d_id = %d AND ol_o_id < %d AND ol_o_id >= (%d - 20)",w_id,d_id,d_next_o_id,d_next_o_id);
printf("\ns2 query:%s\n",tempquery);
	if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
	resp = mysql_use_result(ctx[t_num]);
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
printf("s3|");
fflush(stdout);
		sprintf(tempquery2,"SELECT count(*) FROM stock WHERE s_w_id = %d AND s_i_id = %d AND s_quantity < %d",w_id,ol_i_id,level);
printf("\ns3 query:%s\n",tempquery2);
fflush(stdout);
		if(mysql_query(ctx2,tempquery2)) goto sqlerr2;

printf("s3.1|");
fflush(stdout);
		resp2 = mysql_use_result(ctx2);
		if((row2 = mysql_fetch_row(resp2)) != NULL){
			i_count = atoi(row2[0]);
		}
		else{
			//NO RESULT
			mysql_free_result(resp2);
			goto sqlerr2;
		}
		mysql_free_result(resp2);
/* ] goda */ 

	}

done:
	/*EXEC_SQL CLOSE ord_line;*/
	/*EXEC_SQL COMMIT WORK;*/
printf("sd|");
fflush(stdout);
	if( mysql_commit(ctx[t_num]) ) goto sqlerr;
	if( mysql_commit(ctx2) ) goto sqlerr;

	return (1);

sqlerr:
        fprintf(stderr,"slev1\n");
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);

sqlerr2:
        fprintf(stderr,"slev2\n");
	error(ctx[t_num],mysql_stmt2);
	printf("\nresp:%x,resp2:%x\n",resp,resp2);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	//mysql_stmt_free_result(mysql_stmt);
	mysql_free_result(resp);
	mysql_rollback(ctx[t_num]);
	return (0);
}
