#pragma once
#include "parser/ast.h"
#include "parser/token.h"
#include <string>
#include <stdexcept>
#include <memory>

class ParseError : public std::runtime_error {
public:
    explicit ParseError(const std::string& msg) : std::runtime_error(msg) {}
};

class Parser {
public:
    // Throws ParseError on invalid input.
    static Statement parse(const std::string& sql);

private:
    explicit Parser(std::vector<Token> tokens);

    Statement        parse_stmt();
    Statement        parse_create();
    InsertStmt       parse_insert();
    SelectStmt       parse_select();
    DeleteStmt       parse_delete();
    Statement        parse_drop();
    Statement        parse_show();
    DescribeStmt     parse_describe();
    UpdateStmt       parse_update();
    TruncateStmt     parse_truncate();
    UseDatabaseStmt  parse_use();
    AlterTableStmt   parse_alter();

    // WHERE: returns a single-condition predicate (no AND/OR/NOT).
    // Returns nullptr if no WHERE keyword is present.
    std::unique_ptr<Predicate> try_parse_where();
    std::unique_ptr<Predicate> parse_pred_or();      // used by HAVING only
    std::unique_ptr<Predicate> parse_pred_and();     // used by HAVING only
    std::unique_ptr<Predicate> parse_pred_not();     // used by HAVING only
    std::unique_ptr<Predicate> parse_pred_atom();    // leaf comparison

    std::vector<OrderByClause> parse_order_by();

    // Token helpers
    Token            peek() const;
    Token            advance();
    Token            expect(TokenType t, const std::string& what);
    bool             check(TokenType t) const;
    bool             match(TokenType t);
    bool             is_keyword(const std::string& kw) const;
    std::string      parse_col_ref();

    std::vector<Token> tokens_;
    size_t             pos_ = 0;
};
