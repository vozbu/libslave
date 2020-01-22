#pragma once

#include "decimal.h"

#include <stdint.h>

#include <iterator>
#include <utility>

// convenient utilities for decimal
namespace slave::decimal
{
    using digit_and_count_t = std::pair<digit_t, uint8_t>;

    // @brief range of decimal number digits
    template <typename Traits>
    class Digits
    {
    public:
        // @brief iterate over decimal digits from high to low
        // @value is pair digits value and number of digits
        class Iterator : public std::iterator<std::input_iterator_tag, digit_and_count_t>
        {
        public:
            Iterator(const Decimal& d, size_t pos) : m_value(d), m_pos(pos) {}
            Iterator& operator++() { ++m_pos; return *this; }
            bool operator==(Iterator& other) const { return m_pos == other.m_pos; }
            bool operator!=(Iterator& other) const { return m_pos != other.m_pos; }
            value_type operator*() const { return Traits::read(m_value, m_pos); }
        private:
            const Decimal& m_value;
            size_t m_pos;
        };

        // @brief iterate over decimal digits, from low bit to high
        class ReverseIterator : public std::iterator<std::input_iterator_tag, digit_and_count_t>
        {
        public:
            ReverseIterator(const Decimal& d, size_t pos) : m_value(d), m_pos(pos) {}
            ReverseIterator& operator++() { --m_pos; return *this; }
            bool operator==(ReverseIterator& other) const { return m_pos == other.m_pos; }
            bool operator!=(ReverseIterator& other) const { return m_pos != other.m_pos; }
            value_type operator*() const { return Traits::read(m_value, m_pos - 1); }
        private:
            const Decimal& m_value;
            size_t m_pos;
        };

        using iterator = Iterator;
        using reverse_iterator = ReverseIterator;

        Digits(const Decimal& v) : m_value(v) {}

        // @brief direct iterators
        iterator begin() const { return Iterator(m_value, 0); }
        iterator end() const { return Iterator(m_value, Traits::size(m_value)); }

        // @brief reverse iterators
        reverse_iterator rbegin() const { return ReverseIterator(m_value, Traits::size(m_value)); }
        reverse_iterator rend() const { return ReverseIterator(m_value, 0); }
    private:
        const Decimal& m_value;
    };

    // @brief trait for integer digits range
    struct IntegerDigitsTrait
    {
        static digit_and_count_t read(const Decimal& d, size_t pos);
        static size_t size(const Decimal& d);
    };

    // @brief trait for fractional digits range
    struct FractionalDigitsTrait
    {
        static digit_and_count_t read(const Decimal& d, size_t pos);
        static size_t size(const Decimal& d);
    };

    // @brief integer digits of decimal value
    using IntegerDigits = Digits<IntegerDigitsTrait>;
    // @brief fractional digits of decimal value
    using FractionalDigits = Digits<FractionalDigitsTrait>;
}
