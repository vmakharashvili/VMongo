using MongoDB.Driver;
using System.Collections.Generic;

namespace VMongo
{
    public static class AsyncEnumerableExtensions
    {
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IAsyncCursor<T> asyncCursor)
        {
            while (await asyncCursor.MoveNextAsync())
            {
                foreach (var current in asyncCursor.Current)
                {
                    yield return current;
                }
            }
        }
    }
}
