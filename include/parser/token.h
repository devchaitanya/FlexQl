#pragma once
#include <string>
#include <string_view>
#include <vector>

enum class TokenType {
    // Keywords
    KW_CREATE, KW_TABLE, KW_IF, KW_NOT, KW_EXISTS,
    KW_INSERT, KW_INTO, KW_VALUES,
    KW_SELECT, KW_FROM, KW_WHERE, KW_ORDER, KW_BY, KW_DESC, KW_ASC,
    KW_INNER, KW_JOIN, KW_ON,
    KW_DELETE,
    KW_UPDATE, KW_SET,
    KW_DROP,
    KW_TRUNCATE,
    KW_SHOW, KW_TABLES, KW_DATABASES,
    KW_DESCRIBE,
    KW_DISTINCT, KW_LIMIT, KW_LIKE,
    KW_AND, KW_OR,
    KW_IS, KW_BETWEEN, KW_IN,
    KW_GROUP, KW_HAVING,
    KW_LEFT, KW_OUTER, KW_RIGHT,
    KW_USE, KW_DATABASE,
    KW_ALTER, KW_ADD, KW_COLUMN, KW_MODIFY, KW_RENAME,
    KW_OFFSET,
    KW_AS,
    KW_COLUMNS,
    KW_PRIMARY, KW_KEY, KW_NULL,
    KW_INT, KW_DECIMAL, KW_VARCHAR, KW_TEXT, KW_DATETIME,

    // Literals / identifiers
    IDENTIFIER,
    STRING_LIT,   // 'hello'
    NUMBER_LIT,   // 42 or 3.14

    // Punctuation
    LPAREN, RPAREN, COMMA, SEMICOLON, DOT, STAR,
    EQ, NEQ, GT, GTE, LT, LTE,

    END_OF_INPUT,
    UNKNOWN
};

struct Token {
    TokenType        type;
    std::string      value; // uppercased for keywords/identifiers; empty for literals
    std::string_view sv;    // points into SQL buffer for STRING_LIT / NUMBER_LIT
};

std::vector<Token> tokenize(const std::string& sql);
