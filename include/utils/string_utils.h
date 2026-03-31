#pragma once
#include <string>
#include <algorithm>
#include <cctype>

namespace strutil {

inline std::string trim(const std::string& s) {
    size_t l = s.find_first_not_of(" \t\r\n");
    if (l == std::string::npos) return "";
    size_t r = s.find_last_not_of(" \t\r\n");
    return s.substr(l, r - l + 1);
}

inline std::string to_upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::toupper(c); });
    return s;
}

inline bool iequals(const std::string& a, const std::string& b) {
    return to_upper(a) == to_upper(b);
}

} // namespace strutil
