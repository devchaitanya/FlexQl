#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

// ── Error codes ───────────────────────────────────────────────────────────────
#define FLEXQL_OK    0
#define FLEXQL_ERROR 1

// ── Opaque database handle ────────────────────────────────────────────────────
typedef struct FlexQL FlexQL;

// ── API ───────────────────────────────────────────────────────────────────────

/**
 * Establishes a connection to the FlexQL server.
 * @param host  Hostname or IP (e.g. "127.0.0.1")
 * @param port  Port number (e.g. 9000)
 * @param db    Output: pointer to newly allocated FlexQL handle
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_open(const char *host, int port, FlexQL **db);

/**
 * Closes the connection and frees all resources.
 * @param db  Database handle returned by flexql_open
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_close(FlexQL *db);

/**
 * Executes an SQL statement on the server.
 * For SELECT queries, callback is invoked once per result row.
 * @param db        Open database handle
 * @param sql       NULL-terminated SQL string
 * @param callback  Called for each result row; NULL = no result processing
 * @param arg       Passed as first argument to callback
 * @param errmsg    On error, set to malloc'd error string (free with flexql_free)
 * @return FLEXQL_OK on success, FLEXQL_ERROR on failure
 */
int flexql_exec(FlexQL *db,
                const char *sql,
                int (*callback)(void *arg, int col_count,
                                char **values, char **col_names),
                void *arg,
                char **errmsg);

/**
 * Frees memory allocated by the FlexQL API (e.g. error strings).
 */
void flexql_free(void *ptr);

/**
 * Returns 1 if the last flexql_exec produced column data (SELECT-like), 0 otherwise.
 */
int flexql_last_was_query(FlexQL *db);

/**
 * Returns the number of result columns from the last query, or 0.
 */
int flexql_last_col_count(FlexQL *db);

/**
 * Returns the i-th column name from the last query, or NULL if out of range.
 * The pointer is valid until the next flexql_exec call.
 */
const char *flexql_last_col_name(FlexQL *db, int i);

#ifdef __cplusplus
}
#endif

#endif /* FLEXQL_H */
