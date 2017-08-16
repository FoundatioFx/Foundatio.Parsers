﻿// Copyright © John Gietzen. All Rights Reserved. This source is subject to the MIT license. Please see license.md for more information.

namespace Pegasus.Common
{
    using System;

    /// <summary>
    /// Encapsulates the success or failure of a particular parsing operation along with the result of operation.
    /// </summary>
    /// <typeparam name="T">The type of the parsing operation's result.</typeparam>
    public class ParseResult<T> : IParseResult<T>, IEquatable<ParseResult<T>>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ParseResult{T}"/> class.
        /// </summary>
        /// <remarks>
        /// A non-null parse result indicates success, whereas a null result indicates failure.
        /// </remarks>
        /// <param name="startCursor">The starting cursor of the match.</param>
        /// <param name="endCursor">The ending cursor of the match.</param>
        /// <param name="value">The value of the match.</param>
        public ParseResult(Cursor startCursor, Cursor endCursor, T value)
        {
            this.StartCursor = startCursor;
            this.EndCursor = endCursor;
            this.Value = value;
        }

        /// <summary>
        /// Gets the ending cursor of the match.
        /// </summary>
        public Cursor EndCursor { get; }

        /// <summary>
        /// Gets the starting cursor of the match.
        /// </summary>
        public Cursor StartCursor { get; }

        /// <summary>
        /// Gets the resulting value of the parsing operation.
        /// </summary>
        public T Value { get; }

        /// <summary>
        /// Determines whether two specified parse results have different values.
        /// </summary>
        /// <param name="left">The first <see cref="ParseResult{T}"/> to compare, or null.</param>
        /// <param name="right">The second <see cref="ParseResult{T}"/> to compare, or null.</param>
        /// <returns>true if the value of <paramref name="left"/> is different from the value of <paramref name="right"/>; otherwise, false.</returns>
        public static bool operator !=(ParseResult<T> left, ParseResult<T> right) => !object.Equals(left, right);

        /// <summary>
        /// Determines whether two specified parse results have the same value.
        /// </summary>
        /// <param name="left">The first <see cref="ParseResult{T}"/> to compare, or null.</param>
        /// <param name="right">The second <see cref="ParseResult{T}"/> to compare, or null.</param>
        /// <returns>true if the value of <paramref name="left"/> is the same as the value of <paramref name="right"/>; otherwise, false.</returns>
        public static bool operator ==(ParseResult<T> left, ParseResult<T> right) => object.Equals(left, right);

        /// <summary>
        /// Determines whether the specified object is equal to the current <see cref="ParseResult{T}"/>.
        /// </summary>
        /// <param name="obj">An object to compare with this <see cref="ParseResult{T}"/>.</param>
        /// <returns>true if the objects are considered equal; otherwise, false.</returns>
        public override bool Equals(object obj) => this.Equals(obj as ParseResult<T>);

        /// <summary>
        /// Determines whether the specified <see cref="ParseResult{T}"/> is equal to the current <see cref="ParseResult{T}"/>.
        /// </summary>
        /// <param name="other">A <see cref="ParseResult{T}"/> to compare with this <see cref="ParseResult{T}"/>.</param>
        /// <returns>true if the parse results are considered equal; otherwise, false.</returns>
        public bool Equals(ParseResult<T> other) =>
            !object.ReferenceEquals(other, null) &&
            this.StartCursor == other.StartCursor &&
            this.EndCursor == other.EndCursor &&
            object.Equals(this.Value, other.Value);

        /// <summary>
        /// Serves as a hash function for this <see cref="ParseResult{T}"/>.
        /// </summary>
        /// <returns>A hash code for the current <see cref="ParseResult{T}"/>.</returns>
        public override int GetHashCode()
        {
            int hash = 0x51ED270B;
            hash = (hash * -0x25555529) + this.StartCursor.GetHashCode();
            hash = (hash * -0x25555529) + this.EndCursor.GetHashCode();
            hash = (hash * -0x25555529) + (object.ReferenceEquals(this.Value, null) ? 0 : this.Value.GetHashCode());

            return hash;
        }
    }
}
