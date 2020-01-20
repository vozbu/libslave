#pragma once

#include <stdint.h>

#include <iosfwd>
#include <string>
#include <utility>

namespace slave::decimal
{
    // @brief digits container type
    using digit_t = uint32_t;
    static_assert(sizeof(digit_t) == 4);

    // @brief structure to store mysql decimal value
    // for more details see see https://dev.mysql.com/doc/refman/5.7/en/precision-math-decimal-characteristics.html
    // @note struct should contain only pod - types, because it's internals are used directly (for the instance by memset)
    struct Decimal
    {
        // the max number of digits which can be stored
        static constexpr size_t max_digits = 65;

        uint8_t storage[30]; // each 4 byte are used to store 9 digits and each 1 byte for 2 digits
        uint8_t intg;        // count of digits before point
        uint8_t frac : 7;    // count of digits after point
        uint8_t sign : 1;    // 0 - positive, 1 - negative

#ifdef SLAVE_USE_DOUBLE_AS_DECIMAL
        // implicit conversion to double (used for backward compatibility)
        operator double() const;
#endif
    };

    static_assert(32 == sizeof(Decimal));

    bool operator==(const Decimal& l, const Decimal& r);

    inline bool operator!=(const Decimal& l, const Decimal& r)
    {
        return !(l == r);
    }

    // @brief print decimal to ostream
    std::ostream& operator<<(std::ostream& os, const Decimal& d);

    // @brief strignify decimal value (used in strignify)
    std::string to_string(const Decimal& d);

    // @brief convert decimal to double
    double to_double(const Decimal& d);

#ifdef SLAVE_USE_DOUBLE_AS_DECIMAL
    inline Decimal::operator double() const
    {
        return to_double(*this);
    }
#endif // SLAVE_USE_DOUBLE_AS_DECIMAL
}
