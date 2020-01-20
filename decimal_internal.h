#pragma once

#include "decimal.h"

#include <string>

// internal utilities for decimal (not for public usage)
namespace slave::decimal
{
    // @brief maximum number of digits in one word (4 byte)
    constexpr uint8_t MAX_DIGITS_PER_WORD = 9;

    // @brief number of bytes which need to store x digits
    constexpr uint8_t digits2bytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
    static_assert(MAX_DIGITS_PER_WORD + 1 == sizeof(digits2bytes) / sizeof(digits2bytes[0]));

    // @brief powers of 10 from 0 to MAX_DIGITS_PER_WORD
    constexpr digit_t powers10[] = {1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000};
    static_assert(MAX_DIGITS_PER_WORD + 1 == sizeof(powers10) / sizeof(powers10[0]));

    constexpr digit_t DIGIT_BASE = 1000000000;
    constexpr digit_t DIGIT_MAX  = DIGIT_BASE - 1;

    enum error
    {
        ERR_DECIMAL_OK = 0,
        ERR_DECIMAL_OVERFLOW,
        ERR_DECIMAL_TRUNCATED,
        ERR_DECIMAL_BAD_NUM,
    };

    // @brief unpack decimal from mysql binary
    error from_binary(const char* from, Decimal& to, unsigned precision, unsigned scale);

    // @brief unpack decimal from string
    // @note does not support scientific notation
    error from_string(const char* from, Decimal& to);
}
