#pragma once

#include <cstring>
#include <string>

namespace aux
{

template <class F>
inline void parse_list(const char* s, F&& f, const char* delim = ",")
{
    for (const char* p = s; '\0' != *p;)
    {
        p += ::strspn(p, delim);
        auto q = p + ::strcspn(p, delim);
        if (p != q)
        {
            f(std::string_view(p, q - p));
            p = q;
        }
    }
}

template <class F>
inline void parse_list(const std::string& aList, F&& f, const char* delim = ",")
{
    parse_list(aList.c_str(), std::forward<F>(f), delim);
}

}// namespace aux
