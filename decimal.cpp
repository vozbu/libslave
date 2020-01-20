#include "decimal.h"

#include <alloca.h>
#include <cstring>
#include <ostream>

#include "decimal_supp.h"
#include "decimal_internal.h"

namespace
{
    using slave::decimal::digit_t;

    constexpr double scaler10[] = {1.0, 1e10, 1e20, 1e30, 1e40, 1e50, 1e60, 1e70, 1e80, 1e90};
    constexpr double scaler1[] = {1.0, 10.0, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9};

    inline size_t count_leading_zeroes(uint8_t digits, digit_t val)
    {
        size_t count = 0;
        while (count < digits && val >= slave::decimal::powers10[count]) { ++count; }
        return digits - count;
    }

    // @brief checks that [first1, std::min(std::distance(first1, last1), std::min(std::distance(first2, last2))
    //        is same and tail is zero
    template <typename It, typename T>
    inline bool equal_digits(It first1, It last1, It first2, It last2, T cmp)
    {
        for (; first1 != last1 && first2 != last2; ++first1, ++first2)
        {
            if (!cmp(*first1, *first2))
                return false;
        }
        // expected that tail is zero
        for (; first1 != last1; ++first1)
        {
            if (0 != (*first1).first)
                return false;
        }
        for (; first2 != last2; ++first2)
        {
            if (0 != (*first2).first)
                return false;
        }
        return true;
    }
} // namespace

bool slave::decimal::operator==(const Decimal& l, const Decimal& r)
{
    if (l.sign != r.sign)
        return false;

    // check integer part
    IntegerDigits idigitsl(l);
    IntegerDigits idigitsr(r);

    bool sEqual = equal_digits(idigitsl.rbegin(), idigitsl.rend(), idigitsr.rbegin(), idigitsr.rend(), [](const auto& x, const auto& y) {
        return x.first == y.first;
    });
    if (!sEqual)
        return false;

    FractionalDigits fdigitsl(l);
    FractionalDigits fdigitsr(r);

    return equal_digits(fdigitsl.begin(), fdigitsl.end(), fdigitsr.begin(), fdigitsr.end(), [](const auto& x, const auto& y) {
        if (x.second == y.second)
            return x.first == y.first;

        if (x.second > y.second)
            return y.first * powers10[x.second - y.second] == x.first;
        return x.first * powers10[y.second - x.second] == y.first;
    });
}

std::ostream& slave::decimal::operator<<(std::ostream& os, const Decimal& d)
{
    return os << to_string(d);
}

double slave::decimal::to_double(const Decimal& d)
{
    // duplicate values to improve performance and avoid casting to double
    static constexpr double base = DIGIT_BASE;

    double result = 0.0;
    unsigned exp = 0;

    for (const auto x : IntegerDigits(d))
    {
        result = result * base + x.first;
    }

    for (const auto x : FractionalDigits(d))
    {
        result = result * scaler1[x.second] + x.first;
        exp += x.second;
    }

    result /= scaler10[exp / 10u] * scaler1[exp % 10u];

    return d.sign ? -result : result;
}

std::string slave::decimal::to_string(const Decimal& d)
{
    IntegerDigits idigits(d);

    size_t intg_len = d.intg;
    if (0 != d.intg)
    {
        auto x = *idigits.begin();
        intg_len -= count_leading_zeroes(x.second, x.first);
    }
    size_t len = intg_len + d.frac + d.sign + (0 == intg_len ? 1 : 0) + (0 != d.frac ? 1 : 0);

    char* buf = reinterpret_cast<char*>(alloca(len));
    char *s = buf;

    if (0 != d.sign)
        *s++ = '-';

    // assign 0 at begin if there is no integer part
    if (0 == intg_len)
        *s++ = '0';

    if (0 != d.frac)
    {
        char* s0 = s + intg_len;
        *s0 = '.';
        char* s1 = s0;
        for (auto i : FractionalDigits(d))
        {
            digit_t x = i.first;
            for (size_t j = i.second; 0 != j; --j)
            {
                digit_t y = x / powers10[j - 1];
                *++s1 = '0' + static_cast<char>(y);
                x -= y * powers10[j - 1];
            }
        }

        // cut trailing zeros
        while (s0 != s1 && '0' == *s1) { --s1; }
        len = (s0 == s1) ? (s0 - buf) : ((s1 - buf) + 1);
    }

    if (0 != intg_len)
    {
        char* s1 = s + intg_len;
        for (auto it = idigits.rbegin(), end = idigits.rend(); end != it; ++it)
        {
            auto vc = *it;
            digit_t x = vc.first;
            // for last item there may be less than vc.second digits, since we cat zero leading
            for (size_t j = vc.second; 0 != j && s != s1; --j)
            {
                digit_t y = x / 10;
                *--s1 = '0' + static_cast<char>(x - y * 10);
                x = y;
            }
        }
    }
    return std::string(buf, len);
}
