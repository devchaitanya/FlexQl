#pragma once
#include "common/types.h"
#include <string>

// Wire protocol (all over TCP — matches reference benchmark protocol):
//
//  Client → Server:
//    Raw SQL text terminated by ';'
//
//  Server → Client (SELECT):
//    "ROW <v1> <v2> ...\n"   (one per result row)
//    "END\n"
//
//  Server → Client (non-SELECT success):
//    "OK\n"
//    "END\n"
//
//  Server → Client (error):
//    "ERROR: <message>\n"
//    "END\n"

namespace protocol {

// Maximum size of one SQL statement (16 MB) to prevent OOM from malicious clients.
inline constexpr size_t MAX_SQL_SIZE = 16 * 1024 * 1024;

// Encode a QueryResult into a wire response string.
std::string encode_response(const QueryResult& res);

// Stream a QueryResult to fd in chunks (avoids building a multi-GB string).
bool stream_response(int fd, const QueryResult& res);

// Send all bytes of msg over fd. Works on blocking and non-blocking sockets
// (waits with poll() when the send buffer is full). Returns false on error.
bool send_all(int fd, const std::string& msg);

// Receive a length-prefixed SQL string from fd.
// Returns "" on disconnect / error.
std::string recv_sql(int fd);

// Send a response string to fd.
bool send_response(int fd, const std::string& resp);

// Receive a full response (reads until "END\n" or "OK\n" or "ERR:...\n").
std::string recv_response(int fd);

} // namespace protocol
