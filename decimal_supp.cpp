#include "decimal_supp.h"

#include <cstring>

#include "decimal_internal.h"

using namespace slave;

namespace
{
    inline uint8_t round_up(uint8_t v)
    {
        return (v + decimal::MAX_DIGITS_PER_WORD - 1) / decimal::MAX_DIGITS_PER_WORD;
    }
}

decimal::digit_and_count_t decimal::IntegerDigitsTrait::read(const Decimal& d, size_t pos)
{
    digit_t v = 0;
    uint8_t count = MAX_DIGITS_PER_WORD;
    size_t offset = 0;
    if (0 != (d.intg % MAX_DIGITS_PER_WORD))
    {
        if (0 == pos)
        {
            count = d.intg % MAX_DIGITS_PER_WORD;
            offset = sizeof(Decimal::storage) - digits2bytes[count];
        }
        else
        {
            offset = (pos - 1) * sizeof(v);
        }
    }
    else
    {
        offset = pos * sizeof(v);
    }
    std::memcpy(&v, d.storage + offset, digits2bytes[count]);
    return decimal::digit_and_count_t(v, count);
}

size_t decimal::IntegerDigitsTrait::size(const Decimal& d)
{
    return round_up(d.intg);
}

decimal::digit_and_count_t decimal::FractionalDigitsTrait::read(const Decimal& d, size_t pos)
{
    digit_t v = 0;
    size_t offset = ((d.intg / MAX_DIGITS_PER_WORD) + pos) * sizeof(v);
    uint8_t count = MAX_DIGITS_PER_WORD;
    if (pos == (size(d) - 1))
    {
        count = d.frac % MAX_DIGITS_PER_WORD;
        if (0 == count)
            count = MAX_DIGITS_PER_WORD;
    }
    std::memcpy(&v, d.storage + offset, digits2bytes[count]);
    return decimal::digit_and_count_t(v, count);
}

size_t decimal::FractionalDigitsTrait::size(const Decimal& d)
{
    return round_up(d.frac);
}
