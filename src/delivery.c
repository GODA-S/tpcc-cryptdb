/*
 * -*-C-*-
 * delivery.pc
 * corresponds to A.4 in appendix A
 */

#include <stdio.h>
#include <string.h>
#include <time.h>

#include <mysql.h>

#include "spt_proc.h"
#include "tpc.h"

extern MYSQL **ctx;
extern MYSQL_STMT ***stmt;
extern struct runtime_log query_log[];

#define NNULL ((void *)0)

int delivery( int t_num,
	      int w_id_arg,
	      int o_carrier_id_arg
)
{
	int            w_id = w_id_arg;
	int            o_carrier_id = o_carrier_id_arg;
	int            d_id;
	int            c_id;
	int            no_o_id;
	int           ol_total;
	//goda float           ol_total;
	char            datetime[81];

/* goda [ */
	char		tempquery[500];
	MYSQL_RES *resp = NULL;
	MYSQL_ROW row;
	struct timespec tbuf;
	double runtime;
/* ] goda */ 

	int proceed = 0;

	MYSQL_STMT*   mysql_stmt;
        MYSQL_BIND    param[4];
        MYSQL_BIND    column[1];

	/*EXEC SQL WHENEVER SQLERROR GOTO sqlerr;*/

        gettimestamp(datetime, STRFTIME_FORMAT, TIMESTAMP_LEN);

	/* For each district in warehouse */
	/* printf("W: %d\n", w_id); */

	for (d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
	        proceed = 1;
		/*EXEC_SQL SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
		                FROM new_orders
		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][25];

		memset(param, 0, sizeof(MYSQL_BIND) * 2); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &d_id;
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;*/
/* goda [ */
		sprintf(tempquery,"SELECT MIN(no_o_id) FROM new_orders WHERE no_d_id = %d AND no_w_id = %d",d_id,w_id);
		
		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[25].runCount++;
		query_log[25].runTime += runtime;

		resp = mysql_use_result(ctx[t_num]);
		if((row = mysql_fetch_row(resp)) != NULL){
			if(row[0] != NULL){
				no_o_id = atoi(row[0]);
			}
			else{
				no_o_id = 0;
			}
		}
		else{
			//NO RESULT
			mysql_free_result(resp);
			goto sqlerr;
		}
		mysql_free_result(resp);
/* ] goda */ 

/* goda commentout
		if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
		memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize *
		column[0].buffer_type = MYSQL_TYPE_LONG;
                column[0].buffer = &no_o_id;
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


		if(no_o_id == 0) continue;
		proceed = 2;
		/*EXEC_SQL DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
		  AND no_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][26];

		memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &no_o_id;
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &d_id;
		param[2].buffer_type = MYSQL_TYPE_LONG;
		param[2].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
*/

/* goda [ */
		sprintf(tempquery,"DELETE FROM new_orders WHERE no_o_id = %d AND no_d_id = %d AND no_w_id = %d",no_o_id,d_id,w_id);

		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[26].runCount++;
		query_log[26].runTime += runtime;

/* ] goda */

		proceed = 3;
		/*EXEC_SQL SELECT o_c_id INTO :c_id FROM orders
		                WHERE o_id = :no_o_id AND o_d_id = :d_id
				AND o_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][27];

		memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &no_o_id;
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &d_id;
		param[2].buffer_type = MYSQL_TYPE_LONG;
		param[2].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

		if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
		memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize *
		column[0].buffer_type = MYSQL_TYPE_LONG;
                column[0].buffer = &c_id;
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
		sprintf(tempquery,"SELECT o_c_id FROM orders WHERE o_id = %d AND o_d_id = %d AND o_w_id = %d",no_o_id,d_id,w_id);

		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[27].runCount++;
		query_log[27].runTime += runtime;

		resp = mysql_use_result(ctx[t_num]);
		if((row = mysql_fetch_row(resp)) != NULL){
			c_id = atoi(row[0]);
		}
		else{
			//NO RESULT
			mysql_free_result(resp);
			goto sqlerr;
		}
		mysql_free_result(resp);
/* ] goda */


		proceed = 4;
		/*EXEC_SQL UPDATE orders SET o_carrier_id = :o_carrier_id
		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
				o_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][28];

		memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &o_carrier_id;
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &no_o_id;
		param[2].buffer_type = MYSQL_TYPE_LONG;
		param[2].buffer = &d_id;
		param[3].buffer_type = MYSQL_TYPE_LONG;
		param[3].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
*/


/* goda [ */
		sprintf(tempquery,"UPDATE orders SET o_carrier_id = %d WHERE o_id = %d AND o_d_id = %d AND o_w_id = %d",o_carrier_id,no_o_id,d_id,w_id);

		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[28].runCount++;
		query_log[28].runTime += runtime;
/* ] goda */

		proceed = 5;
		/*EXEC_SQL UPDATE order_line
		                SET ol_delivery_d = :datetime
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
				ol_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][29];

		memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_STRING;
                param[0].buffer = datetime;
                param[0].buffer_length = strlen(datetime);
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &no_o_id;
		param[2].buffer_type = MYSQL_TYPE_LONG;
		param[2].buffer = &d_id;
		param[3].buffer_type = MYSQL_TYPE_LONG;
		param[3].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
		if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
*/

/* goda [ */
		sprintf(tempquery,"UPDATE order_line SET ol_delivery_d = '%s' WHERE ol_o_id = %d AND ol_d_id = %d AND ol_w_id = %d",datetime,no_o_id,d_id,w_id);
		
		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[29].runCount++;
		query_log[29].runTime += runtime;
/* ] goda */

		proceed = 6;
		/*EXEC_SQL SELECT SUM(ol_amount) INTO :ol_total
		                FROM order_line
		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
				AND ol_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][30];

		memset(param, 0, sizeof(MYSQL_BIND) * 3); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_LONG;
		param[0].buffer = &no_o_id;
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &d_id;
		param[2].buffer_type = MYSQL_TYPE_LONG;
		param[2].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
                if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;

		if( mysql_stmt_store_result(mysql_stmt) ) goto sqlerr;
                memset(column, 0, sizeof(MYSQL_BIND) * 1); /* initialize *
                column[0].buffer_type = MYSQL_TYPE_FLOAT;
                column[0].buffer = &ol_total;
		if( mysql_stmt_bind_result(mysql_stmt, column) ) goto sqlerr;
                switch( mysql_stmt_fetch(mysql_stmt) ) {
                    case 0: //SUCCESS
                    case MYSQL_DATA_TRUNCATED:
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
		sprintf(tempquery,"SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = %d AND ol_d_id = %d AND ol_w_id = %d",no_o_id,d_id,w_id);
		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[30].runCount++;
		query_log[30].runTime += runtime;

		resp = mysql_use_result(ctx[t_num]);
		if((row = mysql_fetch_row(resp)) != NULL){
			ol_total = atoi(row[0]);
		}
		else{
			//NO RESULT
			mysql_free_result(resp);
			goto sqlerr;
		}
		mysql_free_result(resp);
/* ] goda */

		proceed = 7;
		/*EXEC_SQL UPDATE customer SET c_balance = c_balance + :ol_total ,
		                             c_delivery_cnt = c_delivery_cnt + 1
		                WHERE c_id = :c_id AND c_d_id = :d_id AND
				c_w_id = :w_id;*/

/* goda commentout
		mysql_stmt = stmt[t_num][31];

		memset(param, 0, sizeof(MYSQL_BIND) * 4); /* initialize *
		param[0].buffer_type = MYSQL_TYPE_FLOAT;
		param[0].buffer = &ol_total;
		param[1].buffer_type = MYSQL_TYPE_LONG;
		param[1].buffer = &c_id;
		param[2].buffer_type = MYSQL_TYPE_LONG;
		param[2].buffer = &d_id;
		param[3].buffer_type = MYSQL_TYPE_LONG;
		param[3].buffer = &w_id;
		if( mysql_stmt_bind_param(mysql_stmt, param) ) goto sqlerr;
                if( mysql_stmt_execute(mysql_stmt) ) goto sqlerr;
*/

/* goda [ */
		sprintf(tempquery,"UPDATE customer SET c_balance = c_balance + %d , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = %d AND c_d_id = %d AND c_w_id = %d",ol_total,c_id,d_id,w_id);
		GetStartTime(&tbuf);
		if(mysql_query(ctx[t_num],tempquery)) goto sqlerr;
		runtime = GetRunTime(&tbuf);
		query_log[31].runCount++;
		query_log[31].runTime += runtime;
/* ] goda */	

		/*EXEC_SQL COMMIT WORK;*/
		if( mysql_commit(ctx[t_num]) ) goto sqlerr;

		/* printf("D: %d, O: %d, time: %d\n", d_id, o_id, tad); */

	}
	/*EXEC_SQL COMMIT WORK;*/
	return (1);

sqlerr:
        fprintf(stderr, "delivery %d:%d\n",t_num,proceed);
	outputQuery(tempquery);
	error(ctx[t_num],mysql_stmt);
        /*EXEC SQL WHENEVER SQLERROR GOTO sqlerrerr;*/
	/*EXEC_SQL ROLLBACK WORK;*/
	mysql_rollback(ctx[t_num]);
sqlerrerr:
	return (0);
}
