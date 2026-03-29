#include "parser/token.h"
#include "utils/string_utils.h"
#include <cctype>
#include <unordered_map>

static const std::unordered_map<std::string, TokenType> KEYWORDS = {
    {"CREATE",   TokenType::KW_CREATE},
    {"TABLE",    TokenType::KW_TABLE},
    {"IF",       TokenType::KW_IF},
    {"NOT",      TokenType::KW_NOT},
    {"EXISTS",   TokenType::KW_EXISTS},
    {"INSERT",   TokenType::KW_INSERT},
    {"INTO",     TokenType::KW_INTO},
    {"VALUES",   TokenType::KW_VALUES},
    {"SELECT",   TokenType::KW_SELECT},
    {"FROM",     TokenType::KW_FROM},
    {"WHERE",    TokenType::KW_WHERE},
    {"ORDER",    TokenType::KW_ORDER},
    {"BY",       TokenType::KW_BY},
    {"DESC",     TokenType::KW_DESC},
    {"ASC",      TokenType::KW_ASC},
    {"INNER",    TokenType::KW_INNER},
    {"JOIN",     TokenType::KW_JOIN},
    {"ON",       TokenType::KW_ON},
    {"DELETE",    TokenType::KW_DELETE},
    {"UPDATE",    TokenType::KW_UPDATE},
    {"SET",       TokenType::KW_SET},
    {"DROP",      TokenType::KW_DROP},
    {"TRUNCATE",  TokenType::KW_TRUNCATE},
    {"SHOW",      TokenType::KW_SHOW},
    {"DISTINCT",  TokenType::KW_DISTINCT},
    {"LIMIT",     TokenType::KW_LIMIT},
    {"LIKE",      TokenType::KW_LIKE},
    {"AND",       TokenType::KW_AND},
    {"OR",        TokenType::KW_OR},
    {"IS",        TokenType::KW_IS},
    {"BETWEEN",   TokenType::KW_BETWEEN},
    {"IN",        TokenType::KW_IN},
    {"GROUP",     TokenType::KW_GROUP},
    {"HAVING",    TokenType::KW_HAVING},
    {"LEFT",      TokenType::KW_LEFT},
    {"OUTER",     TokenType::KW_OUTER},
    {"RIGHT",     TokenType::KW_RIGHT},
    {"USE",       TokenType::KW_USE},
    {"ALTER",     TokenType::KW_ALTER},
    {"ADD",       TokenType::KW_ADD},
    {"COLUMN",    TokenType::KW_COLUMN},
    {"MODIFY",    TokenType::KW_MODIFY},
    {"RENAME",    TokenType::KW_RENAME},
    {"OFFSET",    TokenType::KW_OFFSET},
    {"AS",        TokenType::KW_AS},
    {"COLUMNS",   TokenType::KW_COLUMNS},
    {"TABLES",    TokenType::KW_TABLES},
    {"DATABASES", TokenType::KW_DATABASES},
    {"DATABASE",  TokenType::KW_DATABASE},
    {"DESCRIBE",  TokenType::KW_DESCRIBE},
    {"PRIMARY",   TokenType::KW_PRIMARY},
    {"KEY",      TokenType::KW_KEY},
    {"NULL",     TokenType::KW_NULL},
    {"INT",      TokenType::KW_INT},
    {"DECIMAL",  TokenType::KW_DECIMAL},
    {"VARCHAR",  TokenType::KW_VARCHAR},
    {"TEXT",     TokenType::KW_TEXT},
    {"DATETIME", TokenType::KW_DATETIME},
};

std::vector<Token> tokenize(const std::string& sql) {
    std::vector<Token> tokens;
    tokens.reserve(sql.size() / 5); // rough estimate: ~5 chars/token
    size_t i = 0;
    size_t n = sql.size();

    while (i < n) {
        // Skip whitespace
        if (std::isspace((unsigned char)sql[i])) { ++i; continue; }

        // String literal 'text'  — zero-copy fast path (no escape handling)
        if (sql[i] == '\'') {
            size_t start = ++i; // skip opening quote
            size_t end   = sql.find('\'', start);
            if (end != std::string::npos) {
                tokens.push_back({TokenType::STRING_LIT, {}, {sql.data() + start, end - start}});
                i = end + 1;
            } else {
                // Unterminated string — push empty view, consume rest
                tokens.push_back({TokenType::STRING_LIT, {}, {}});
                i = n;
            }
            continue;
        }

        // Number literal (integer or decimal, optional leading minus)
        if (std::isdigit((unsigned char)sql[i]) ||
            (sql[i] == '-' && i + 1 < n && std::isdigit((unsigned char)sql[i+1]))) {
            size_t start = i++;
            while (i < n && (std::isdigit((unsigned char)sql[i]) || sql[i] == '.'))
                ++i;
            tokens.push_back({TokenType::NUMBER_LIT, {}, {sql.data() + start, i - start}});
            continue;
        }

        // Identifier or keyword — sv always points into the SQL buffer (stable lifetime)
        if (std::isalpha((unsigned char)sql[i]) || sql[i] == '_') {
            size_t start = i;
            while (i < n && (std::isalnum((unsigned char)sql[i]) || sql[i] == '_'))
                ++i;
            std::string_view raw_sv{sql.data() + start, i - start};
            std::string upper = strutil::to_upper(std::string(raw_sv));
            auto it = KEYWORDS.find(upper);
            if (it != KEYWORDS.end())
                tokens.push_back({it->second, std::move(upper), raw_sv});
            else
                tokens.push_back({TokenType::IDENTIFIER, std::string(raw_sv), raw_sv});
            continue;
        }

        // Operators and punctuation
        char c = sql[i];
        if (c == '!' && i + 1 < n && sql[i+1] == '=') {
            tokens.push_back({TokenType::NEQ, "!=", {}}); i += 2; continue;
        }
        if (c == '>' && i + 1 < n && sql[i+1] == '=') {
            tokens.push_back({TokenType::GTE, ">=", {}}); i += 2; continue;
        }
        if (c == '<' && i + 1 < n && sql[i+1] == '=') {
            tokens.push_back({TokenType::LTE, "<=", {}}); i += 2; continue;
        }

        switch (c) {
            case '(': tokens.push_back({TokenType::LPAREN,    "(", {}}); break;
            case ')': tokens.push_back({TokenType::RPAREN,    ")", {}}); break;
            case ',': tokens.push_back({TokenType::COMMA,     ",", {}}); break;
            case ';': tokens.push_back({TokenType::SEMICOLON, ";", {}}); break;
            case '.': tokens.push_back({TokenType::DOT,       ".", {}}); break;
            case '*': tokens.push_back({TokenType::STAR,      "*", {}}); break;
            case '=': tokens.push_back({TokenType::EQ,        "=", {}}); break;
            case '>': tokens.push_back({TokenType::GT,        ">", {}}); break;
            case '<': tokens.push_back({TokenType::LT,        "<", {}}); break;
            default:  tokens.push_back({TokenType::UNKNOWN, std::string(1,c), {}}); break;
        }
        ++i;
    }

    tokens.push_back({TokenType::END_OF_INPUT, "", {}});
    return tokens;
}
