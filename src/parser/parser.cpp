#include "parser/parser.h"
#include "parser/ast.h"
#include "utils/string_utils.h"
#include <optional>
#include <cctype>
#include <cstring>
#include <memory>

static std::string tok_str(const Token& t) {
    return t.sv.empty() ? t.value : std::string(t.sv);
}

// ── Fast INSERT parser ────────────────────────────────────────────────────────
static std::optional<InsertStmt> fast_parse_insert(const std::string& sql) {
    const char* p   = sql.data();
    const char* end = p + sql.size();

    auto match_kw = [&](const char* kw) -> bool {
        while (p < end && std::isspace((unsigned char)*p)) ++p;
        for (const char* k = kw; *k; ++k, ++p) {
            if (p >= end || std::toupper((unsigned char)*p) != (unsigned char)*k)
                return false;
        }
        if (p < end && (std::isalnum((unsigned char)*p) || *p == '_'))
            return false;
        return true;
    };

    if (!match_kw("INSERT")) return std::nullopt;
    if (!match_kw("INTO"))   return std::nullopt;

    while (p < end && std::isspace((unsigned char)*p)) ++p;
    const char* tstart = p;
    while (p < end && (std::isalnum((unsigned char)*p) || *p == '_')) ++p;
    if (p == tstart) return std::nullopt;
    std::string tname(tstart, p);
    for (auto& c : tname) c = (char)std::toupper((unsigned char)c);

    // Optional column list: INSERT INTO t (col1, col2, ...) VALUES ...
    InsertStmt stmt;
    stmt.table = std::move(tname);
    {
        const char* saved = p;
        while (p < end && std::isspace((unsigned char)*p)) ++p;
        if (p < end && *p == '(') {
            ++p;
            while (p < end && *p != ')') {
                while (p < end && std::isspace((unsigned char)*p)) ++p;
                if (p >= end || *p == ')') break;
                const char* cs = p;
                while (p < end && (std::isalnum((unsigned char)*p) || *p == '_')) ++p;
                if (p == cs) return std::nullopt; // not an identifier
                std::string cname(cs, p);
                for (auto& c : cname) c = (char)std::toupper((unsigned char)c);
                stmt.col_list.push_back(std::move(cname));
                while (p < end && std::isspace((unsigned char)*p)) ++p;
                if (p < end && *p == ',') ++p;
            }
            if (p >= end || *p != ')') return std::nullopt;
            ++p;
        } else {
            p = saved; // no column list, restore
        }
    }

    if (!match_kw("VALUES")) return std::nullopt;
    stmt.flat_values.reserve(5 * 25000); // 125K for 25K rows × 5 cols

    int first_row_ncols = 0;
    bool first_row = true;

    while (true) {
        while (p < end && std::isspace((unsigned char)*p)) ++p;
        if (p >= end || *p != '(') break;
        ++p;

        int row_val_count = 0;
        while (p < end && *p != ')') {
            while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == ',')) ++p;
            if (p >= end || *p == ')') break;

            if (*p == '\'') {
                const char* s = ++p;
                const char* q = (const char*)std::memchr(s, '\'', (size_t)(end - s));
                if (!q) return std::nullopt;
                stmt.flat_values.push_back({s, (size_t)(q - s)});
                p = q + 1;
            } else if ((unsigned)(unsigned char)*p - '0' < 10u ||
                       (*p == '-' && p+1 < end && (unsigned)(unsigned char)*(p+1) - '0' < 10u)) {
                const char* s = p++;
                while (p < end && ((unsigned)(unsigned char)*p - '0' < 10u || *p == '.')) ++p;
                stmt.flat_values.push_back({s, (size_t)(p - s)});
            } else {
                return std::nullopt;
            }
            ++row_val_count;
        }

        if (p >= end) return std::nullopt;
        ++p; // ')'

        if (first_row) { first_row_ncols = row_val_count; first_row = false; }

        while (p < end && std::isspace((unsigned char)*p)) ++p;
        if (p < end && *p == ',') { ++p; continue; }
        break;
    }

    stmt.flat_ncols = first_row_ncols;
    return stmt;
}

// ── Construction ──────────────────────────────────────────────────────────────
Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

Statement Parser::parse(const std::string& sql) {
    const char* p = sql.data();
    while (*p && std::isspace((unsigned char)*p)) ++p;
    if ((*p == 'I' || *p == 'i') && (*(p+1) == 'N' || *(p+1) == 'n')) {
        auto opt = fast_parse_insert(sql);
        if (opt) return std::move(*opt);
    }
    Parser pr(tokenize(sql));
    return pr.parse_stmt();
}

// ── Token helpers ─────────────────────────────────────────────────────────────
Token Parser::peek() const {
    if (pos_ < tokens_.size()) return tokens_[pos_];
    return {TokenType::END_OF_INPUT, "", {}};
}

Token Parser::advance() {
    Token t = peek();
    if (pos_ < tokens_.size()) ++pos_;
    return t;
}

bool Parser::check(TokenType t) const { return peek().type == t; }

bool Parser::match(TokenType t) {
    if (check(t)) { advance(); return true; }
    return false;
}

Token Parser::expect(TokenType t, const std::string& what) {
    if (!check(t))
        throw ParseError("expected " + what + " but got '" + peek().value + "'");
    return advance();
}

bool Parser::is_keyword(const std::string& kw) const {
    return peek().value == kw;
}

std::string Parser::parse_col_ref() {
    Token first = expect(TokenType::IDENTIFIER, "column or table name");
    std::string name = strutil::to_upper(first.value);
    if (check(TokenType::DOT)) {
        advance();
        Token second = expect(TokenType::IDENTIFIER, "column name after '.'");
        return name + "." + strutil::to_upper(second.value);
    }
    return name;
}

// ── Top-level dispatcher ──────────────────────────────────────────────────────
Statement Parser::parse_stmt() {
    Token t = peek();
    if (t.type == TokenType::KW_CREATE)   return parse_create();
    if (t.type == TokenType::KW_INSERT)   return parse_insert();
    if (t.type == TokenType::KW_SELECT)   return parse_select();
    if (t.type == TokenType::KW_DELETE)   return parse_delete();
    if (t.type == TokenType::KW_UPDATE)   return parse_update();
    if (t.type == TokenType::KW_DROP)     return parse_drop();
    if (t.type == TokenType::KW_TRUNCATE) return parse_truncate();
    if (t.type == TokenType::KW_SHOW)     return parse_show();
    if (t.type == TokenType::KW_DESCRIBE) return parse_describe();
    if (t.type == TokenType::KW_USE)      return parse_use();
    if (t.type == TokenType::KW_ALTER)    return parse_alter();
    throw ParseError("unknown statement starting with '" + t.value + "'");
}

// ── CREATE TABLE | CREATE DATABASE ───────────────────────────────────────────
Statement Parser::parse_create() {
    expect(TokenType::KW_CREATE, "CREATE");
    // CREATE DATABASE [IF NOT EXISTS] name  — single-db server, just acknowledge
    if (check(TokenType::KW_DATABASE)) {
        advance();
        match(TokenType::KW_IF); match(TokenType::KW_NOT); match(TokenType::KW_EXISTS);
        UseDatabaseStmt s;
        if (check(TokenType::IDENTIFIER)) s.db_name = strutil::to_upper(advance().value);
        match(TokenType::SEMICOLON);
        return s;
    }
    expect(TokenType::KW_TABLE, "TABLE");

    CreateTableStmt stmt;

    if (check(TokenType::KW_IF)) {
        advance();
        expect(TokenType::KW_NOT,    "NOT");
        expect(TokenType::KW_EXISTS, "EXISTS");
        stmt.if_not_exists = true;
    }

    Token name_tok = expect(TokenType::IDENTIFIER, "table name");
    stmt.schema.table_name = strutil::to_upper(name_tok.value);

    expect(TokenType::LPAREN, "(");

    while (!check(TokenType::RPAREN) && !check(TokenType::END_OF_INPUT)) {
        ColumnDef col;
        col.name = strutil::to_upper(expect(TokenType::IDENTIFIER, "column name").value);

        Token tp = advance();
        switch (tp.type) {
            case TokenType::KW_INT:      col.type = ColumnType::INT;      break;
            case TokenType::KW_DECIMAL:  col.type = ColumnType::DECIMAL;  break;
            case TokenType::KW_VARCHAR:  col.type = ColumnType::VARCHAR;  break;
            case TokenType::KW_TEXT:     col.type = ColumnType::TEXT;     break;
            case TokenType::KW_DATETIME: col.type = ColumnType::DATETIME; break;
            default: throw ParseError("unknown type: " + tp.value);
        }
        if (check(TokenType::LPAREN)) {
            advance();
            while (!check(TokenType::RPAREN) && !check(TokenType::END_OF_INPUT))
                advance();
            advance();
        }

        while (!check(TokenType::COMMA) && !check(TokenType::RPAREN) &&
               !check(TokenType::END_OF_INPUT)) {
            if (check(TokenType::KW_PRIMARY)) {
                advance();
                expect(TokenType::KW_KEY, "KEY");
                col.primary_key = true;
            } else if (check(TokenType::KW_NOT)) {
                advance();
                expect(TokenType::KW_NULL, "NULL");
                col.not_null = true;
            } else {
                break;
            }
        }

        if (col.primary_key)
            stmt.schema.pk_col_index = (int)stmt.schema.columns.size();
        stmt.schema.columns.push_back(std::move(col));

        if (!match(TokenType::COMMA)) break;
    }

    expect(TokenType::RPAREN, ")");
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── INSERT INTO name [(col,...)] VALUES (v1,v2,...)[,(v1,v2,...)]* ───────────
InsertStmt Parser::parse_insert() {
    expect(TokenType::KW_INSERT, "INSERT");
    expect(TokenType::KW_INTO,   "INTO");

    InsertStmt stmt;
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);

    // Optional column list: INSERT INTO t (col1, col2) VALUES ...
    if (check(TokenType::LPAREN)) {
        advance();
        while (!check(TokenType::RPAREN) && !check(TokenType::END_OF_INPUT)) {
            stmt.col_list.push_back(
                strutil::to_upper(expect(TokenType::IDENTIFIER, "column name").value));
            if (!match(TokenType::COMMA)) break;
        }
        expect(TokenType::RPAREN, ")");
    }

    expect(TokenType::KW_VALUES, "VALUES");

    do {
        expect(TokenType::LPAREN, "(");
        std::vector<std::string_view> row;
        while (!check(TokenType::RPAREN) && !check(TokenType::END_OF_INPUT)) {
            Token v = advance();
            if (v.type == TokenType::NUMBER_LIT || v.type == TokenType::STRING_LIT) {
                row.push_back(v.sv);
            } else if (v.type == TokenType::IDENTIFIER) {
                row.push_back(v.sv.empty() ? std::string_view(v.value) : v.sv);
            } else {
                throw ParseError("unexpected token in VALUES: " + tok_str(v));
            }
            if (!match(TokenType::COMMA)) break;
        }
        expect(TokenType::RPAREN, ")");
        stmt.rows.push_back(std::move(row));
    } while (match(TokenType::COMMA));

    match(TokenType::SEMICOLON);
    return stmt;
}

// ── Compound WHERE predicate parsing ─────────────────────────────────────────
// Grammar (precedence low→high):
//   pred_or  ::= pred_and ( OR pred_and )*
//   pred_and ::= pred_not ( AND pred_not )*
//   pred_not ::= NOT pred_not | pred_atom
//   pred_atom::= col op val | col IS [NOT] NULL | col BETWEEN v AND v
//              | col [NOT] IN ( v, ... ) | col LIKE pat | ( pred_or )

std::unique_ptr<Predicate> Parser::try_parse_where() {
    if (!check(TokenType::KW_WHERE)) return nullptr;
    advance(); // consume WHERE
    return parse_pred_or();
}

std::unique_ptr<Predicate> Parser::parse_pred_or() {
    auto left = parse_pred_and();
    while (check(TokenType::KW_OR)) {
        advance();
        auto right = parse_pred_and();
        auto node = std::make_unique<Predicate>();
        node->kind = Predicate::OR_NODE;
        node->children.push_back(std::move(left));
        node->children.push_back(std::move(right));
        left = std::move(node);
    }
    return left;
}

std::unique_ptr<Predicate> Parser::parse_pred_and() {
    auto left = parse_pred_not();
    while (check(TokenType::KW_AND)) {
        advance();
        auto right = parse_pred_not();
        auto node = std::make_unique<Predicate>();
        node->kind = Predicate::AND_NODE;
        node->children.push_back(std::move(left));
        node->children.push_back(std::move(right));
        left = std::move(node);
    }
    return left;
}

std::unique_ptr<Predicate> Parser::parse_pred_not() {
    if (check(TokenType::KW_NOT)) {
        advance();
        auto child = parse_pred_not();
        auto node = std::make_unique<Predicate>();
        node->kind = Predicate::NOT_NODE;
        node->children.push_back(std::move(child));
        return node;
    }
    return parse_pred_atom();
}

std::unique_ptr<Predicate> Parser::parse_pred_atom() {
    // Parenthesised sub-expression
    if (check(TokenType::LPAREN)) {
        advance();
        auto inner = parse_pred_or();
        expect(TokenType::RPAREN, ")");
        return inner;
    }

    // FUNC(arg) expression used in HAVING — treat as a column name "FUNC(ARG)"
    std::string col;
    if (check(TokenType::IDENTIFIER) &&
        pos_ + 1 < tokens_.size() &&
        tokens_[pos_ + 1].type == TokenType::LPAREN)
    {
        std::string fn = strutil::to_upper(advance().value);
        advance(); // '('
        std::string arg;
        if (check(TokenType::STAR)) { advance(); arg = "*"; }
        else arg = parse_col_ref();
        expect(TokenType::RPAREN, ")");
        col = fn + "(" + arg + ")";
    } else {
        col = parse_col_ref();
    }
    auto p = std::make_unique<Predicate>();
    p->kind = Predicate::LEAF;
    p->col  = col;

    // IS [NOT] NULL
    if (check(TokenType::KW_IS)) {
        advance();
        bool not_null = false;
        if (check(TokenType::KW_NOT)) { advance(); not_null = true; }
        expect(TokenType::KW_NULL, "NULL");
        p->op = not_null ? "IS NOT NULL" : "IS NULL";
        return p;
    }

    // [NOT] BETWEEN  /  [NOT] IN
    bool negate = false;
    if (check(TokenType::KW_NOT)) {
        advance();
        negate = true;
    }

    if (check(TokenType::KW_BETWEEN)) {
        advance();
        Token lo = advance();
        p->val  = tok_str(lo);
        expect(TokenType::KW_AND, "AND");
        Token hi = advance();
        p->val2 = tok_str(hi);
        p->op = negate ? "NOT BETWEEN" : "BETWEEN";
        return p;
    }

    if (check(TokenType::KW_IN)) {
        advance();
        expect(TokenType::LPAREN, "(");
        while (!check(TokenType::RPAREN) && !check(TokenType::END_OF_INPUT)) {
            Token v = advance();
            p->in_vals.push_back(tok_str(v));
            if (!match(TokenType::COMMA)) break;
        }
        expect(TokenType::RPAREN, ")");
        p->op = negate ? "NOT IN" : "IN";
        return p;
    }

    if (negate)
        throw ParseError("expected BETWEEN or IN after NOT in WHERE clause");

    // LIKE
    if (check(TokenType::KW_LIKE)) {
        advance();
        p->op = "LIKE";
        Token val = advance();
        p->val = tok_str(val);
        return p;
    }

    // Standard comparison operators
    Token op = advance();
    switch (op.type) {
        case TokenType::EQ:  p->op = "=";  break;
        case TokenType::NEQ: p->op = "!="; break;
        case TokenType::GT:  p->op = ">";  break;
        case TokenType::GTE: p->op = ">="; break;
        case TokenType::LT:  p->op = "<";  break;
        case TokenType::LTE: p->op = "<="; break;
        default: throw ParseError("expected comparison operator in WHERE");
    }

    Token val = advance();
    if (val.type == TokenType::NUMBER_LIT || val.type == TokenType::STRING_LIT ||
        val.type == TokenType::IDENTIFIER || val.type == TokenType::KW_NULL)
        p->val = tok_str(val);
    else
        throw ParseError("expected value in WHERE");

    return p;
}

// ── ORDER BY col [ASC|DESC] [, col [ASC|DESC]]* ───────────────────────────────
std::vector<OrderByClause> Parser::parse_order_by() {
    expect(TokenType::KW_ORDER, "ORDER");
    expect(TokenType::KW_BY,    "BY");
    std::vector<OrderByClause> keys;
    do {
        OrderByClause ob;
        ob.col  = parse_col_ref();
        ob.desc = false;
        if (check(TokenType::KW_DESC)) { advance(); ob.desc = true; }
        else if (check(TokenType::KW_ASC)) { advance(); }
        keys.push_back(std::move(ob));
    } while (match(TokenType::COMMA));
    return keys;
}

// ── SELECT ────────────────────────────────────────────────────────────────────
SelectStmt Parser::parse_select() {
    expect(TokenType::KW_SELECT, "SELECT");
    SelectStmt stmt;

    if (check(TokenType::KW_DISTINCT)) {
        advance();
        stmt.distinct = true;
    }

    // Aggregate function detection
    if (check(TokenType::IDENTIFIER) &&
        pos_ + 1 < tokens_.size() &&
        tokens_[pos_ + 1].type == TokenType::LPAREN)
    {
        std::string fn = strutil::to_upper(tokens_[pos_].value);
        if (fn == "COUNT" || fn == "SUM" || fn == "AVG" ||
            fn == "MIN"   || fn == "MAX")
        {
            advance();
            advance(); // '('
            std::string agg_col;
            if (check(TokenType::STAR)) {
                advance(); agg_col = "*";
            } else {
                agg_col = parse_col_ref();
            }
            expect(TokenType::RPAREN, ")");
            stmt.agg_func = fn;
            stmt.agg_col  = agg_col;
            stmt.cols     = {fn + "(" + agg_col + ")"};
            expect(TokenType::KW_FROM, "FROM");
            stmt.table = strutil::to_upper(
                expect(TokenType::IDENTIFIER, "table name").value);
            stmt.where = try_parse_where();
            match(TokenType::SEMICOLON);
            return stmt;
        }
    }

    // Column list — each entry may be a plain col ref, FUNC(col/*), or expr AS alias
    if (check(TokenType::STAR)) {
        advance();
        stmt.cols = {"*"};
        stmt.col_aliases = {""};
    } else {
        do {
            std::string col_expr;
            std::string alias;

            // Detect FUNC(arg) — identifier immediately followed by '('
            if (check(TokenType::IDENTIFIER) &&
                pos_ + 1 < tokens_.size() &&
                tokens_[pos_ + 1].type == TokenType::LPAREN)
            {
                std::string fn = strutil::to_upper(advance().value); // function name
                advance(); // '('
                std::string arg;
                if (check(TokenType::STAR)) { advance(); arg = "*"; }
                else arg = parse_col_ref();
                expect(TokenType::RPAREN, ")");
                col_expr = fn + "(" + arg + ")";
                // Record aggregate for GROUP BY handling
                if (stmt.agg_func.empty()) {
                    stmt.agg_func = fn;
                    stmt.agg_col  = arg;
                }
            } else {
                col_expr = parse_col_ref();
            }

            // Optional AS alias
            if (check(TokenType::KW_AS)) {
                advance();
                alias = strutil::to_upper(expect(TokenType::IDENTIFIER, "alias name").value);
            } else if (check(TokenType::IDENTIFIER)) {
                // Implicit alias without AS keyword
                alias = strutil::to_upper(advance().value);
            }

            stmt.cols.push_back(col_expr);
            stmt.col_aliases.push_back(alias);
        } while (match(TokenType::COMMA));

        // Reset agg_func/col if we also have non-aggregate columns (GROUP BY handles it)
        if (stmt.cols.size() > 1 || stmt.group_by.empty()) {
            stmt.agg_func.clear();
            stmt.agg_col.clear();
        }
    }

    expect(TokenType::KW_FROM, "FROM");
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);

    // JOIN (INNER / LEFT [OUTER])
    if (check(TokenType::KW_INNER) || check(TokenType::KW_LEFT)) {
        if (check(TokenType::KW_LEFT)) {
            advance();
            stmt.left_join = true;
            match(TokenType::KW_OUTER); // optional OUTER
        } else {
            advance(); // INNER
        }
        expect(TokenType::KW_JOIN, "JOIN");
        stmt.join_table = strutil::to_upper(expect(TokenType::IDENTIFIER, "join table name").value);
        expect(TokenType::KW_ON, "ON");
        stmt.join_left  = parse_col_ref();
        expect(TokenType::EQ,   "=");
        stmt.join_right = parse_col_ref();
    }

    stmt.where = try_parse_where();

    // GROUP BY
    if (check(TokenType::KW_GROUP)) {
        advance();
        expect(TokenType::KW_BY, "BY");
        do {
            stmt.group_by.push_back(parse_col_ref());
        } while (match(TokenType::COMMA));

        // HAVING
        if (check(TokenType::KW_HAVING)) {
            advance();
            stmt.having = parse_pred_or();
        }
    }

    if (check(TokenType::KW_ORDER))
        stmt.order_by = parse_order_by();

    if (check(TokenType::KW_LIMIT)) {
        advance();
        Token lim = expect(TokenType::NUMBER_LIT, "LIMIT count");
        stmt.limit = std::stoi(tok_str(lim));
    }

    if (check(TokenType::KW_OFFSET)) {
        advance();
        Token off = expect(TokenType::NUMBER_LIT, "OFFSET count");
        stmt.offset = std::stoi(tok_str(off));
    }

    match(TokenType::SEMICOLON);
    return stmt;
}

// ── DELETE FROM name [WHERE ...] ──────────────────────────────────────────────
DeleteStmt Parser::parse_delete() {
    expect(TokenType::KW_DELETE, "DELETE");
    expect(TokenType::KW_FROM,   "FROM");
    DeleteStmt stmt;
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);
    stmt.where = try_parse_where();
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── DROP TABLE [IF EXISTS] name ───────────────────────────────────────────────
Statement Parser::parse_drop() {
    expect(TokenType::KW_DROP, "DROP");
    // DROP DATABASE [IF EXISTS] name  — no-op on single-db server
    if (check(TokenType::KW_DATABASE)) {
        advance();
        match(TokenType::KW_IF); match(TokenType::KW_EXISTS);
        UseDatabaseStmt s;
        if (check(TokenType::IDENTIFIER)) s.db_name = strutil::to_upper(advance().value);
        match(TokenType::SEMICOLON);
        return s;
    }
    expect(TokenType::KW_TABLE, "TABLE");
    DropTableStmt stmt;
    if (check(TokenType::KW_IF)) {
        advance();
        expect(TokenType::KW_EXISTS, "EXISTS");
        stmt.if_exists = true;
    }
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── SHOW TABLES | SHOW DATABASES | SHOW COLUMNS FROM tbl ─────────────────────
Statement Parser::parse_show() {
    expect(TokenType::KW_SHOW, "SHOW");
    Token t = advance();
    if (t.type == TokenType::KW_TABLES)    { match(TokenType::SEMICOLON); return ShowTablesStmt{}; }
    if (t.type == TokenType::KW_DATABASES) { match(TokenType::SEMICOLON); return ShowDatabasesStmt{}; }
    if (t.type == TokenType::KW_DATABASE)  { match(TokenType::SEMICOLON); return ShowDatabasesStmt{}; }
    // SHOW COLUMNS FROM tbl  (synonym for DESCRIBE tbl)
    if (t.type == TokenType::KW_COLUMNS || t.value == "COLUMNS") {
        match(TokenType::KW_FROM);  // optional FROM keyword
        DescribeStmt d;
        if (check(TokenType::IDENTIFIER))
            d.table = strutil::to_upper(advance().value);
        match(TokenType::SEMICOLON);
        return d;
    }
    throw ParseError("expected TABLES, DATABASES, or COLUMNS after SHOW");
}

// ── DESCRIBE table_name ───────────────────────────────────────────────────────
DescribeStmt Parser::parse_describe() {
    expect(TokenType::KW_DESCRIBE, "DESCRIBE");
    DescribeStmt stmt;
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── UPDATE table SET col=val [, ...] [WHERE ...] ──────────────────────────────
UpdateStmt Parser::parse_update() {
    expect(TokenType::KW_UPDATE, "UPDATE");
    UpdateStmt stmt;
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);
    expect(TokenType::KW_SET, "SET");

    do {
        std::string col = strutil::to_upper(expect(TokenType::IDENTIFIER, "column name").value);
        expect(TokenType::EQ, "=");
        Token val = advance();
        if (val.type != TokenType::NUMBER_LIT && val.type != TokenType::STRING_LIT &&
            val.type != TokenType::IDENTIFIER)
            throw ParseError("expected value in SET");
        stmt.assignments.emplace_back(col, tok_str(val));
    } while (match(TokenType::COMMA));

    stmt.where = try_parse_where();
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── TRUNCATE TABLE table_name ─────────────────────────────────────────────────
TruncateStmt Parser::parse_truncate() {
    expect(TokenType::KW_TRUNCATE, "TRUNCATE");
    expect(TokenType::KW_TABLE,    "TABLE");
    TruncateStmt stmt;
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── USE [DATABASE] db_name ────────────────────────────────────────────────────
UseDatabaseStmt Parser::parse_use() {
    expect(TokenType::KW_USE, "USE");
    match(TokenType::KW_DATABASE); // optional DATABASE keyword
    match(TokenType::KW_DATABASES);
    UseDatabaseStmt stmt;
    if (check(TokenType::IDENTIFIER))
        stmt.db_name = strutil::to_upper(advance().value);
    match(TokenType::SEMICOLON);
    return stmt;
}

// ── ALTER TABLE tbl ADD/DROP/MODIFY COLUMN col_def ───────────────────────────
AlterTableStmt Parser::parse_alter() {
    expect(TokenType::KW_ALTER, "ALTER");
    expect(TokenType::KW_TABLE, "TABLE");
    AlterTableStmt stmt;
    stmt.table = strutil::to_upper(expect(TokenType::IDENTIFIER, "table name").value);

    Token action = advance();
    match(TokenType::KW_COLUMN); // optional COLUMN keyword

    if (action.type == TokenType::KW_ADD) {
        stmt.action = AlterTableStmt::ADD_COLUMN;
        stmt.col_def.name = strutil::to_upper(
            expect(TokenType::IDENTIFIER, "column name").value);
        Token tp = advance();
        switch (tp.type) {
            case TokenType::KW_INT:      stmt.col_def.type = ColumnType::INT;      break;
            case TokenType::KW_DECIMAL:  stmt.col_def.type = ColumnType::DECIMAL;  break;
            case TokenType::KW_VARCHAR:  stmt.col_def.type = ColumnType::VARCHAR;  break;
            case TokenType::KW_TEXT:     stmt.col_def.type = ColumnType::TEXT;     break;
            case TokenType::KW_DATETIME: stmt.col_def.type = ColumnType::DATETIME; break;
            default: throw ParseError("unknown column type: " + tp.value);
        }
        if (check(TokenType::LPAREN)) {
            advance();
            while (!check(TokenType::RPAREN) && !check(TokenType::END_OF_INPUT)) advance();
            advance();
        }
    } else if (action.value == "DROP") {
        stmt.action   = AlterTableStmt::DROP_COLUMN;
        stmt.drop_col = strutil::to_upper(
            expect(TokenType::IDENTIFIER, "column name").value);
    } else if (action.type == TokenType::KW_MODIFY || action.value == "MODIFY") {
        stmt.action = AlterTableStmt::MODIFY_COLUMN;
        stmt.col_def.name = strutil::to_upper(
            expect(TokenType::IDENTIFIER, "column name").value);
        Token tp = advance();
        switch (tp.type) {
            case TokenType::KW_INT:      stmt.col_def.type = ColumnType::INT;      break;
            case TokenType::KW_DECIMAL:  stmt.col_def.type = ColumnType::DECIMAL;  break;
            case TokenType::KW_VARCHAR:  stmt.col_def.type = ColumnType::VARCHAR;  break;
            case TokenType::KW_TEXT:     stmt.col_def.type = ColumnType::TEXT;     break;
            case TokenType::KW_DATETIME: stmt.col_def.type = ColumnType::DATETIME; break;
            default: throw ParseError("unknown column type: " + tp.value);
        }
    } else {
        throw ParseError("ALTER TABLE: expected ADD, DROP, or MODIFY");
    }

    match(TokenType::SEMICOLON);
    return stmt;
}
